"""DuckDB writing helpers: atomic chunked save with Windows/YandexDisk retries."""

import gc
import logging
import os
import shutil
import tempfile
import time
import uuid
from pathlib import Path
from typing import List

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


def _duckdb_sidecar_paths(db_path: Path) -> List[Path]:
    """Return DuckDB sidecar files that can linger after a connection closes."""
    return [
        db_path.with_name(db_path.name + '.wal'),
        db_path.with_name(db_path.name + '.tmp'),
    ]


def _unlink_with_retry(path: Path, attempts: int = 10, delay: float = 0.2) -> None:
    """Remove a file, tolerating short-lived Windows/YandexDisk file locks."""
    for attempt in range(attempts):
        try:
            path.unlink()
            return
        except FileNotFoundError:
            return
        except PermissionError:
            if attempt == attempts - 1:
                raise
            gc.collect()
            time.sleep(delay * (attempt + 1))


def _copy_with_retry(source: Path, target: Path, attempts: int = 10, delay: float = 0.2) -> None:
    """Copy source to target, retrying transient sync-client locks."""
    for attempt in range(attempts):
        try:
            shutil.copy2(source, target)
            return
        except PermissionError:
            if attempt == attempts - 1:
                raise
            gc.collect()
            time.sleep(delay * (attempt + 1))


def _cleanup_temp_duckdb_files(tmp_path: Path, strict: bool = True) -> None:
    """Remove temporary DuckDB file and its sidecars."""
    for path in [tmp_path] + _duckdb_sidecar_paths(tmp_path):
        if path.exists():
            try:
                _unlink_with_retry(path)
            except OSError:
                if strict:
                    raise
                logger.warning(f"Could not remove stale temp DuckDB file: {path}")


def _cleanup_duckdb_sidecars(db_path: Path, strict: bool = True) -> None:
    """Remove only DuckDB sidecar files, leaving the main database in place."""
    for path in _duckdb_sidecar_paths(db_path):
        if path.exists():
            try:
                _unlink_with_retry(path)
            except OSError:
                if strict:
                    raise
                logger.warning(f"Could not remove DuckDB sidecar file: {path}")


def _duckdb_build_path(output_path: Path) -> Path:
    """Choose a non-synced temp location for building DuckDB files."""
    base_dir = Path(
        os.environ.get(
            'MGIMO_DUCKDB_TMPDIR',
            Path(tempfile.gettempdir()) / 'mgimo_foreign_trade_duckdb'
        )
    )
    base_dir.mkdir(parents=True, exist_ok=True)
    return base_dir / f"{output_path.stem}.{uuid.uuid4().hex}{output_path.suffix}"


def save_to_duckdb(df: pd.DataFrame, output_path: Path, table_name: str = 'unified_trade_data', chunk_size: int = 100000):
    """
    Save DataFrame to DuckDB database in chunks to conserve memory.

    Args:
        df: DataFrame to save
        output_path: Path to DuckDB file
        table_name: Name of the table in database
        chunk_size: Number of rows to write per chunk
    """
    logger.info(f"Saving merged data to DuckDB: {output_path}")

    if df.empty:
        logger.warning("Input DataFrame is empty. Nothing to save to DuckDB.")
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Build DuckDB outside YandexDisk/synced folders. DuckDB creates WAL files
    # while writing, and sync clients on Windows can lock those sidecars long
    # enough to break checkpoint/replace operations.
    tmp_path = _duckdb_build_path(output_path)
    backup_path = None
    legacy_tmp_path = output_path.with_name(output_path.name + '.tmp')
    for stale_path in [legacy_tmp_path]:
        if stale_path.exists() or any(path.exists() for path in _duckdb_sidecar_paths(stale_path)):
            logger.warning(f"Removing stale temp DuckDB files from a previous failed run: {stale_path}")
            _cleanup_temp_duckdb_files(stale_path, strict=False)

    conn = None
    try:
        conn = duckdb.connect(str(tmp_path))

        # Ensure PERIOD is normalized (time set to 00:00:00) before saving
        # We'll cast it to DATE in DuckDB to remove time component completely
        if 'PERIOD' in df.columns:
            # Convert to datetime and normalize to remove time (set to 00:00:00)
            df['PERIOD'] = pd.to_datetime(df['PERIOD'], errors='coerce').dt.normalize()

        # Create the table and insert the first chunk
        # Explicitly cast PERIOD to DATE in DuckDB to ensure no time component
        first_chunk = df.iloc[:chunk_size]
        conn.register('first_chunk_df', first_chunk)
        if 'PERIOD' in first_chunk.columns:
            conn.execute(f"""
                CREATE TABLE {table_name} AS
                SELECT
                    * EXCLUDE (PERIOD),
                    CAST(PERIOD AS DATE) AS PERIOD
                FROM first_chunk_df
            """)
        else:
            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM first_chunk_df")
        conn.unregister('first_chunk_df')
        logger.info(f"  ... created table and inserted first {len(first_chunk):,} rows")
        logger.info(f"  ... PERIOD column saved as DATE type (no time component)")

        # Insert the rest of the data in chunks using the efficient append method
        for i in range(chunk_size, len(df), chunk_size):
            chunk = df.iloc[i:i + chunk_size].copy()  # Explicit copy to avoid SettingWithCopyWarning
            # Ensure PERIOD is normalized before appending
            if 'PERIOD' in chunk.columns:
                chunk['PERIOD'] = pd.to_datetime(chunk['PERIOD'], errors='coerce').dt.normalize()
            # Use INSERT with explicit DATE cast for PERIOD to ensure no time component
            if 'PERIOD' in chunk.columns:
                conn.register('chunk_df', chunk)
                conn.execute(f"""
                    INSERT INTO {table_name}
                    SELECT
                        * EXCLUDE (PERIOD),
                        CAST(PERIOD AS DATE) AS PERIOD
                    FROM chunk_df
                """)
                conn.unregister('chunk_df')
            else:
                conn.append(table_name, chunk)
            logger.info(f"  ... inserted {i + len(chunk):,} / {len(df):,} rows")

        # Get row count for verification
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        row_count = result[0]

        # Flush WAL contents into the main DB before copying it to a synced
        # folder. On Windows, the WAL sidecar can remain briefly locked even
        # after close, so cleanup below is best-effort.
        conn.execute("CHECKPOINT")
        conn.close()
        conn = None
        gc.collect()

        if row_count != len(df):
            logger.warning(f"Row count mismatch! Expected {len(df):,}, but DuckDB table has {row_count:,}.")

        _cleanup_duckdb_sidecars(tmp_path, strict=False)

        # YandexDisk can lock freshly copied staging files and make os.replace
        # unreliable. Copy the closed local DuckDB file directly, but keep a
        # local backup of the previous database so a failed copy can be rolled
        # back without losing the last good database.
        if output_path.exists():
            backup_path = _duckdb_build_path(output_path)
            _copy_with_retry(output_path, backup_path)

        try:
            _copy_with_retry(tmp_path, output_path)
        except Exception:
            if backup_path and backup_path.exists():
                logger.warning(f"Restoring previous DuckDB database after failed copy: {output_path}")
                _copy_with_retry(backup_path, output_path)
            elif output_path.exists():
                try:
                    _unlink_with_retry(output_path)
                except OSError:
                    logger.warning(f"Could not remove partial DuckDB file after failed copy: {output_path}")
            raise

        _cleanup_temp_duckdb_files(tmp_path, strict=False)
        if backup_path:
            _cleanup_temp_duckdb_files(backup_path, strict=False)
        logger.info(f"Successfully saved {row_count:,} rows to {output_path}")

    except Exception as e:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                logger.warning("Could not close failed DuckDB connection cleanly.")
            finally:
                conn = None
                gc.collect()

        # Remove the incomplete temp file so no stale data is left behind.
        # The original output_path was never touched, so it remains valid.
        try:
            _cleanup_temp_duckdb_files(tmp_path, strict=False)
            if backup_path:
                _cleanup_temp_duckdb_files(backup_path, strict=False)
            _cleanup_temp_duckdb_files(legacy_tmp_path, strict=False)
            logger.info(f"Removed incomplete temp DuckDB files for: {tmp_path}")
        except OSError:
            logger.warning(f"Could not remove temp DuckDB files (manual cleanup needed): {tmp_path}")
        logger.error(f"Failed to save to DuckDB: {e}")
        raise


__all__ = ["save_to_duckdb"]
