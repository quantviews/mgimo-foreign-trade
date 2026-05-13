#!/usr/bin/env python3
"""
Script to merge processed parquet files (data_processed/) and save to DuckDB.

This script:
1. Reads processed parquet files from data_processed/ folder
2. Validates each dataset against the data model schema
3. Optionally, loads and transforms Comtrade data for missing countries
4. Merges all datasets into one unified dataset
5. Excludes countries specified in --exclude-countries argument
6. start_year argument to filter data from this year onwards
7. Optionally loads nowcast from data_processed/nowcast/nowcast.parquet (TYPE=pred; on by default, disable with --no-nowcast). Any pred row whose (PERIOD, STRANA, TNVED, NAPR) key already exists in factual rows (national/comtrade) is dropped so nowcast fills only gaps.
8. Optionally loads fizob parquet files into fizob_index (on by default, disable with --no-fizob).
9. Saves the merged dataset to DuckDB format in db/unified_trade_data.duckdb
"""

import pandas as pd
import duckdb
from pathlib import Path
import logging
import re
import argparse
import json
import gc
import os
import shutil
import tempfile
import time
import uuid
from typing import Dict, List

from core.normalization_rules import (
    add_tnved_columns,
    apply_special_edizm_cases,
    get_special_edizm_aliases,
    normalize_tnved_code,
    standardize_edizm_columns,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define the expected schema from data_model.md
EXPECTED_SCHEMA = {
    'NAPR': 'object',          # VARCHAR - торговый поток (ИМ/ЭК)
    'PERIOD': 'datetime64[ns]', # DATE - отчетный период (normalized to 00:00:00, DuckDB will save as DATE)
    'STRANA': 'object',         # VARCHAR - страна-отчет (ISO код)
    'TNVED': 'object',          # VARCHAR - код ТН ВЭД (8-10 знаков)
    'EDIZM': 'object',          # VARCHAR - единица измерения
    'EDIZM_ISO': 'object',      # VARCHAR - ISO код единицы измерения (опционально)
    'STOIM': 'float64',         # DECIMAL - стоимость в ТЫСЯЧАХ USD
    'NETTO': 'float64',         # DECIMAL - вес нетто в кг
    'KOL': 'float64',           # DECIMAL - количество в дополнительной единице
    'TNVED4': 'object',         # VARCHAR - первые 4 знака TNVED
    'TNVED6': 'object',         # VARCHAR - первые 6 знаков TNVED
    'TNVED2': 'object'          # VARCHAR - первые 2 знака TNVED
}

def validate_schema(df: pd.DataFrame, filename: str) -> bool:
    """
    Validate DataFrame against expected schema.
    
    Args:
        df: DataFrame to validate
        filename: Name of the file for error reporting
        
    Returns:
        True if schema is valid, False otherwise


    """
    logger.info(f"Validating schema for {filename}")
    
    # Check if all required columns are present
    missing_cols = set(EXPECTED_SCHEMA.keys()) - set(df.columns)
    if missing_cols:
        logger.error(f"Missing columns in {filename}: {missing_cols}")
        return False
    
    # Check for extra columns
    extra_cols = set(df.columns) - set(EXPECTED_SCHEMA.keys())
    if extra_cols:
        logger.warning(f"Extra columns in {filename}: {extra_cols}")
    
    # Check data types (only for non-null values)
    for col, expected_type in EXPECTED_SCHEMA.items():
        if col in df.columns:
            actual_type = df[col].dtype
            
            # Convert period to date if it's not already
            if col == 'PERIOD':
                try:
                    # Convert to datetime and normalize to remove time component
                    # We'll use datetime64[ns] but normalized (time set to 00:00:00)
                    # DuckDB will recognize it as DATE when saving
                    if 'datetime' not in str(actual_type):
                        df[col] = pd.to_datetime(df[col]).dt.normalize()
                    elif actual_type != 'datetime64[ns]':
                        df[col] = pd.to_datetime(df[col]).dt.normalize()
                    else:
                        # Already datetime, just normalize to remove time
                        df[col] = df[col].dt.normalize()
                    actual_type = df[col].dtype
                except Exception as e:
                    logger.error(f"Failed to convert PERIOD to date in {filename}: {e}")
                    return False
            
            if actual_type != expected_type:
                logger.error(f"Column {col} has wrong type in {filename}: expected {expected_type}, got {actual_type}")
                return False
    
    # Validate specific values
    if 'NAPR' in df.columns:
        invalid_napr = df[~df['NAPR'].isin(['ИМ', 'ЭК'])]['NAPR'].unique()
        if len(invalid_napr) > 0:
            logger.error(f"Invalid NAPR values in {filename}: {invalid_napr}")
            return False
    
    if 'PERIOD' in df.columns:
        invalid_periods = df[df['PERIOD'].isnull()]
        if len(invalid_periods) > 0:
            logger.error(f"Null periods found in {filename}")
            return False
    
    logger.info(f"Schema validation passed for {filename}")
    return True


def smoke_check_merged_dataset(df: pd.DataFrame, label: str = "merged dataset") -> bool:
    """
    Smoke-checks the final merged DataFrame before it is written to DuckDB.

    Checks (all failures are logged as ERROR):
      1. Non-empty result after merge.
      2. All required columns from EXPECTED_SCHEMA are present.
      3. PERIOD is datetime64 and has no null values.
      4. NAPR contains only valid values: 'ИМ' or 'ЭК'.

    Returns True if every check passes, False otherwise.
    Callers should abort saving when False is returned.
    """
    passed = True

    # 1. Non-empty
    if df.empty:
        logger.error(f"SMOKE CHECK FAILED [{label}]: result is empty (0 rows after merge)")
        return False

    # 2. Required columns
    required = frozenset(EXPECTED_SCHEMA.keys())
    missing = required - set(df.columns)
    if missing:
        logger.error(
            f"SMOKE CHECK FAILED [{label}]: missing required columns: {sorted(missing)}"
        )
        passed = False

    # 3. PERIOD type and nullability
    if 'PERIOD' in df.columns:
        if not pd.api.types.is_datetime64_any_dtype(df['PERIOD']):
            logger.error(
                f"SMOKE CHECK FAILED [{label}]: PERIOD is not datetime64 "
                f"(got {df['PERIOD'].dtype})"
            )
            passed = False
        elif df['PERIOD'].isna().any():
            null_count = int(df['PERIOD'].isna().sum())
            logger.error(
                f"SMOKE CHECK FAILED [{label}]: PERIOD has {null_count:,} null values"
            )
            passed = False

    # 4. NAPR values
    if 'NAPR' in df.columns:
        invalid_napr = set(df['NAPR'].dropna().unique()) - {'ИМ', 'ЭК'}
        if invalid_napr:
            bad_rows = int(df['NAPR'].isin(invalid_napr).sum())
            logger.error(
                f"SMOKE CHECK FAILED [{label}]: invalid NAPR values in {bad_rows:,} rows: "
                f"{sorted(invalid_napr)}"
            )
            passed = False

    if passed:
        period_min = df['PERIOD'].min() if 'PERIOD' in df.columns else '?'
        period_max = df['PERIOD'].max() if 'PERIOD' in df.columns else '?'
        napr_vals = sorted(df['NAPR'].dropna().unique()) if 'NAPR' in df.columns else []
        logger.info(
            f"SMOKE CHECK PASSED [{label}]: {len(df):,} rows | "
            f"PERIOD [{period_min} – {period_max}] | "
            f"NAPR {napr_vals}"
        )

    return passed


def load_and_validate_file(file_path: Path, start_year: int = None) -> pd.DataFrame:
    """
    Load parquet file and validate schema.
    
    Args:
        file_path: Path to parquet file
        start_year: Optional year to filter data from
        
    Returns:
        Validated DataFrame or None if validation fails
    """
    try:
        logger.info(f"Loading {file_path}")
        df = pd.read_parquet(file_path)
        
        if start_year:
            if 'PERIOD' not in df.columns:
                logger.warning(f"Cannot filter by year: {file_path.name} has no PERIOD column.")
            else:
                # Convert to datetime and normalize to remove time component
                if 'datetime' not in str(df['PERIOD'].dtype):
                    df['PERIOD'] = pd.to_datetime(df['PERIOD'], errors='coerce').dt.normalize()
                else:
                    df['PERIOD'] = df['PERIOD'].dt.normalize()
                
                initial_rows = len(df)
                df = df[df['PERIOD'].dt.year >= start_year].copy()
                if len(df) < initial_rows:
                    logger.info(f"Filtered {file_path.name} by start_year >= {start_year}. Kept {len(df)} of {initial_rows} rows.")

        # Skip schema validation for fizob files (they have a different schema)
        if not file_path.name.startswith('fizob_'):
            # Validate schema
            if not validate_schema(df, file_path.name):
                logger.error(f"Schema validation failed for {file_path.name}")
                return None
        else:
            logger.info(f"Skipping schema validation for {file_path.name} (fizob file with different schema)")
        
        logger.info(f"Successfully loaded {file_path.name}: {len(df)} rows")
        return df
        
    except Exception as e:
        logger.error(f"Failed to load {file_path}: {e}")
        return None

def generate_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate derived TNVED columns if they don't exist or validate them.
    
    Args:
        df: DataFrame to process
        
    Returns:
        DataFrame with validated/generated derived columns
    """


    return add_tnved_columns(df)


def transform_nowcast_to_unified(df: pd.DataFrame, start_year: int = None) -> pd.DataFrame:
    """
    Transform nowcast parquet to unified schema and keep only TYPE='pred' rows.
    """
    required_cols = {'STRANA', 'PERIOD', 'TNVED', 'NAPR', 'TYPE', 'STOIM', 'NETTO'}
    missing_cols = required_cols - set(df.columns)
    if missing_cols:
        logger.warning(f"Nowcast file is missing required columns: {missing_cols}. Skipping nowcast.")
        return pd.DataFrame()

    nowcast_df = df.copy()
    nowcast_df['TYPE'] = nowcast_df['TYPE'].astype(str).str.strip().str.lower()
    nowcast_df = nowcast_df[nowcast_df['TYPE'] == 'pred'].copy()

    if nowcast_df.empty:
        logger.info("Nowcast file has no rows with TYPE='pred'.")
        return pd.DataFrame()

    nowcast_df['PERIOD'] = pd.to_datetime(nowcast_df['PERIOD'], errors='coerce').dt.normalize()
    nowcast_df.dropna(subset=['PERIOD'], inplace=True)

    if start_year:
        initial_rows = len(nowcast_df)
        nowcast_df = nowcast_df[nowcast_df['PERIOD'].dt.year >= start_year].copy()
        if len(nowcast_df) < initial_rows:
            logger.info(
                f"Filtered nowcast by start_year >= {start_year}. "
                f"Kept {len(nowcast_df)} of {initial_rows} rows."
            )

    if nowcast_df.empty:
        logger.info("Nowcast is empty after filtering.")
        return pd.DataFrame()

    nowcast_df = generate_derived_columns(nowcast_df)
    nowcast_df['STRANA'] = nowcast_df['STRANA'].astype(str).str.upper()
    nowcast_df['NAPR'] = nowcast_df['NAPR'].astype(str).str.strip()

    # Bring nowcast rows to unified structure.
    nowcast_df['EDIZM'] = None
    nowcast_df['EDIZM_ISO'] = None
    nowcast_df['KOL'] = None
    nowcast_df['STOIM'] = pd.to_numeric(nowcast_df['STOIM'], errors='coerce')
    nowcast_df['NETTO'] = pd.to_numeric(nowcast_df['NETTO'], errors='coerce')

    unified_cols = list(EXPECTED_SCHEMA.keys()) + ['TYPE']
    for col in unified_cols:
        if col not in nowcast_df.columns:
            nowcast_df[col] = None

    return nowcast_df[unified_cols]


def transform_fizob_to_unified(df: pd.DataFrame, file_stem: str) -> pd.DataFrame:
    """
    Transform a fizob parquet DataFrame to unified fizob_index schema.
    Schema: STRANA, NAPR, PERIOD, tn_level, tn_code, fizob, fizob_bp
    """
    df = df.copy()
    if 'PERIOD' in df.columns:
        df['PERIOD'] = pd.to_datetime(df['PERIOD'], errors='coerce').dt.normalize()
    
    if file_stem == 'fizob_total':
        # Total level: aggregated across all TNVED, tn_level=0, tn_code='0'
        if 'fizob' not in df.columns or 'fizob_bp' not in df.columns:
            logger.warning(f"fizob_total missing fizob/fizob_bp columns, skipping")
            return pd.DataFrame()
        out = df[['STRANA', 'NAPR', 'PERIOD']].copy()
        out['tn_level'] = 0
        out['tn_code'] = df['TNVED2'].fillna(0).astype(int).astype(str) if 'TNVED2' in df.columns else '0'
        out['fizob'] = df['fizob'].values
        out['fizob_bp'] = df['fizob_bp'].values
        return out
    
    # Level-specific: fizob_2, fizob_4, fizob_6
    mapping = {
        'fizob_2': (2, 'TNVED2', 'fizob2', 'fizob2_bp'),
        'fizob2': (2, 'TNVED2', 'fizob2', 'fizob2_bp'),
        'fizob_4': (4, 'TNVED4', 'fizob4', 'fizob4_bp'),
        'fizob4': (4, 'TNVED4', 'fizob4', 'fizob4_bp'),
        'fizob_6': (6, 'TNVED6', 'fizob6', 'fizob6_bp'),
        'fizob6': (6, 'TNVED6', 'fizob6', 'fizob6_bp'),
    }
    if file_stem not in mapping:
        logger.warning(f"Unknown fizob file stem '{file_stem}', skipping")
        return pd.DataFrame()
    
    level, tnved_col, fizob_col, fizob_bp_col = mapping[file_stem]
    for col in [tnved_col, fizob_col, fizob_bp_col]:
        if col not in df.columns:
            logger.warning(f"{file_stem} missing column {col}, skipping")
            return pd.DataFrame()
    
    out = df[['STRANA', 'NAPR', 'PERIOD', tnved_col]].copy()
    out = out.rename(columns={tnved_col: 'tn_code'})
    out['tn_level'] = level
    out['fizob'] = df[fizob_col].values
    out['fizob_bp'] = df[fizob_bp_col].values
    return out


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

def save_reference_tables(conn: duckdb.DuckDBPyConnection, project_root: Path):
    """
    Save reference tables (TNVED names, country names) as separate tables in DuckDB.
    This normalizes the database structure and reduces data duplication.
    
    Args:
        conn: DuckDB connection
        project_root: Path to project root for metadata loading
    """
    logger.info("Creating reference tables...")
    
    # Save TNVED mappings
    tnved_mappings = load_tnved_mapping(project_root)
    if tnved_mappings:
        # Create unified TNVED reference table
        tnved_refs = []
        for level_name, mapping in tnved_mappings.items():
            # Extract level number from key like 'tnved2', 'tnved10', etc.
            level_num = level_name.replace('tnved', '').replace('TNVED', '')
            try:
                level_int = int(level_num)
            except ValueError:
                logger.warning(f"Could not parse TNVED level from '{level_name}', skipping...")
                continue
            for code, code_data in mapping.items():
                # code_data is now a dict with 'name' and 'translated' keys
                name = code_data.get('name', '')
                translated = code_data.get('translated', False)
                
                if not name:
                    continue
                
                # Prepare code to match format in unified_trade_data
                # IMPORTANT: For ALL levels (2, 4, 6, 8, 10) - codes should match original structure (with leading zeros)
                # No normalization (removal of leading zeros) should be applied
                code_str = str(code).strip()
                
                # For all levels: use original code structure (with leading zeros)
                # First ensure code is at least 10 digits by padding with zeros on the RIGHT if needed
                if len(code_str) >= 10:
                    code_padded = code_str[:10]
                else:
                    code_padded = code_str + '0' * (10 - len(code_str))
                
                # Extract the appropriate length for this level (from original structure, preserving leading zeros)
                if level_int == 2:
                    normalized_code = code_padded[:2]
                elif level_int == 4:
                    normalized_code = code_padded[:4]
                elif level_int == 6:
                    normalized_code = code_padded[:6]
                elif level_int == 8:
                    normalized_code = code_padded[:8]
                elif level_int == 10:
                    normalized_code = code_padded[:10]
                else:
                    normalized_code = code_str
                
                tnved_refs.append({
                    'TNVED_CODE': normalized_code,
                    'TNVED_LEVEL': level_int,
                    'TNVED_NAME': name,
                    'TRANSLATED': translated
                })
        
        if tnved_refs:
            tnved_df = pd.DataFrame(tnved_refs)
            # Remove duplicates, keeping official mappings (translated=False) over translations (translated=True)
            # Sort so that translated=False comes first, then drop duplicates
            tnved_df = tnved_df.sort_values('TRANSLATED').drop_duplicates(
                subset=['TNVED_CODE', 'TNVED_LEVEL'],
                keep='first'
            )
            
            conn.register('tnved_ref_df', tnved_df)
            conn.execute("""
                CREATE TABLE tnved_reference AS 
                SELECT DISTINCT TNVED_CODE, TNVED_LEVEL, TNVED_NAME, TRANSLATED
                FROM tnved_ref_df
                ORDER BY TNVED_LEVEL, TNVED_CODE
            """)
            conn.unregister('tnved_ref_df')
            
            official_count = (tnved_df['TRANSLATED'] == False).sum()
            translated_count = (tnved_df['TRANSLATED'] == True).sum()
            logger.info(f"  ... created tnved_reference table with {len(tnved_df)} rows "
                       f"({official_count} official, {translated_count} translated)")
            
            # Create index for faster joins
            conn.execute("CREATE INDEX idx_tnved_ref_code_level ON tnved_reference(TNVED_CODE, TNVED_LEVEL)")
    
    # Save country name mappings
    strana_mapping = load_strana_mapping(project_root)
    if strana_mapping:
        country_refs = [{'STRANA': k, 'STRANA_NAME': v} for k, v in strana_mapping.items()]
        country_df = pd.DataFrame(country_refs)
        conn.register('country_ref_df', country_df)
        conn.execute("""
            CREATE TABLE country_reference AS 
            SELECT DISTINCT STRANA, STRANA_NAME
            FROM country_ref_df
            ORDER BY STRANA
        """)
        conn.unregister('country_ref_df')
        logger.info(f"  ... created country_reference table with {len(country_df)} rows")
        
        # Create index for faster joins
        conn.execute("CREATE INDEX idx_country_ref_strana ON country_reference(STRANA)")
    
    # Create convenience view that joins main table with reference tables
    logger.info("Creating convenience view with joined reference data...")
    conn.execute("""
        CREATE OR REPLACE VIEW unified_trade_data_enriched AS
        SELECT 
            t.*,
            c.STRANA_NAME AS COUNTRY_NAME,
            t2.TNVED_NAME AS TNVED2_NAME,
            t4.TNVED_NAME AS TNVED4_NAME,
            t6.TNVED_NAME AS TNVED6_NAME,
            t8.TNVED_NAME AS TNVED8_NAME,
            COALESCE(t10.TNVED_NAME, t8.TNVED_NAME) AS TNVED_NAME,
            COALESCE(t10.TRANSLATED, t8.TRANSLATED) AS TNVED_TRANSLATED,
            DENSE_RANK() OVER (
                PARTITION BY t.STRANA, t.TNVED, t.NAPR
                ORDER BY t.PERIOD DESC
            ) AS period_rank
        FROM unified_trade_data t
        LEFT JOIN country_reference c ON t.STRANA = c.STRANA
        LEFT JOIN tnved_reference t2 ON t.TNVED2 = t2.TNVED_CODE AND t2.TNVED_LEVEL = 2
        LEFT JOIN tnved_reference t4 ON t.TNVED4 = t4.TNVED_CODE AND t4.TNVED_LEVEL = 4
        LEFT JOIN tnved_reference t6 ON t.TNVED6 = t6.TNVED_CODE AND t6.TNVED_LEVEL = 6
        LEFT JOIN tnved_reference t8 ON t.TNVED8 = t8.TNVED_CODE AND t8.TNVED_LEVEL = 8
        LEFT JOIN tnved_reference t10 ON t.TNVED = t10.TNVED_CODE AND t10.TNVED_LEVEL = 10
    """)
    logger.info("  ... created unified_trade_data_enriched view")

def load_partner_mapping(project_root: Path) -> Dict[int, str]:
    """Loads Comtrade partner code (M49) to ISO2 mapping from JSON."""
    mapping_file = project_root / "metadata" / "comtrate-partnerAreas.json"
    if not mapping_file.exists():
        logger.error(f"Partner mapping file not found at {mapping_file}")
        return {}
    
    with open(mapping_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # M49 codes are numeric, ISO2 are strings
    mapping = {
        int(item['id']): item.get('PartnerCodeIsoAlpha2')
        for item in data.get('results', []) if item.get('PartnerCodeIsoAlpha2')
    }
    return mapping

def load_edizm_mapping(project_root: Path) -> Dict[int, str]:
    """Loads Comtrade qtyCode to qtyAbbr mapping from JSON."""
    mapping_file = project_root / "metadata" / "comtradte-QuantityUnits.json"
    if not mapping_file.exists():
        logger.error(f"Edizm mapping file not found at {mapping_file}")
        return {}
    
    with open(mapping_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    mapping = {
        item['qtyCode']: item.get('qtyAbbr')
        for item in data.get('results', [])
    }
    return mapping

def load_common_edizm_mapping(project_root: Path) -> Dict[str, Dict[str, str]]:
    """Loads a comprehensive, case-insensitive mapping for EDIZM values."""
    mapping_file = project_root / "metadata" / "edizm.csv"
    if not mapping_file.exists():
        logger.error(f"Common EDIZM mapping file not found at {mapping_file}")
        return {}

    try:
        # Read all columns as strings and prevent pandas from interpreting "NA" as NaN
        df = pd.read_csv(mapping_file, dtype=str, na_filter=False)
        
        # Standardize column names and values to uppercase for case-insensitive matching
        df.columns = df.columns.str.upper()
        df['KOD'] = df['KOD'].str.replace('"', '').str.strip()
        df['NAME'] = df['NAME'].str.upper().str.strip()

        # Create canonical records from the main edizm file (vectorized)
        canonical_records = {}
        # Use itertuples for better performance than iterrows
        for row in df.itertuples(index=False):
            record = {'KOD': row.KOD, 'NAME': row.NAME}
            canonical_records[row.NAME] = record
            # Also map by KOD if it exists
            if row.KOD:
                canonical_records[row.KOD] = record

        final_mapping = {}
        # Populate mapping from the edizm file itself (KOD, NAME) - vectorized
        for row in df.itertuples(index=False):
            record = canonical_records[row.NAME]
            final_mapping[row.NAME] = record
            if row.KOD:
                final_mapping[row.KOD] = record
        
        # Add a comprehensive set of aliases. All keys must be uppercase.
        aliases = {
            # Russian abbreviations
            'ШТ': canonical_records.get('ШТУКА'),
            'КГ': canonical_records.get('КИЛОГРАММ'),
            'Т': canonical_records.get('ТОННА, МЕТРИЧЕСКАЯ ТОННА (1000 КГ)'),
            'М': canonical_records.get('МЕТР'),
            'М2': canonical_records.get('КВАДРАТНЫЙ МЕТР'),
            'М3': canonical_records.get('КУБИЧЕСКИЙ МЕТР'),
            'Л': canonical_records.get('ЛИТР'),
            'Г': canonical_records.get('ГРАММ'),
            'КАРАТ': canonical_records.get('МЕТРИЧЕСКИЙ КАРАТ(1КАРАТ=2*10(-4)КГ'),

            # Comtrade abbreviations (from comtradte-QuantityUnits.json)
            'KG': canonical_records.get('КИЛОГРАММ'),
            'U': canonical_records.get('ШТУКА'),
            'L': canonical_records.get('ЛИТР'),
            'M': canonical_records.get('МЕТР'),  # Latin M (meter)
            'M²': canonical_records.get('КВАДРАТНЫЙ МЕТР'),
            'M2': canonical_records.get('КВАДРАТНЫЙ МЕТР'),  # M2 without superscript
            'M3': canonical_records.get('КУБИЧЕСКИЙ МЕТР'),
            '2U': canonical_records.get('ПАРА'),
            'CARAT': canonical_records.get('МЕТРИЧЕСКИЙ КАРАТ(1КАРАТ=2*10(-4)КГ'),
            '1000U': canonical_records.get('ТЫСЯЧА ШТУК'),
            'G': canonical_records.get('ГРАММ'),
            '1000 KWH': canonical_records.get('1000 КИЛОВАТТ-ЧАС'),
            '1000 L': canonical_records.get('1000 ЛИТРОВ'),
            '1000 KG': canonical_records.get('ТОННА, МЕТРИЧЕСКАЯ ТОННА (1000 КГ)'),
            'L ALC 100%': canonical_records.get('ЛИТР ЧИСТОГО (100%) СПИРТА'),
            # Additional Comtrade codes
            'BBL': canonical_records.get('БАРРЕЛЬ'),  # Barrel (code 11, if exists)
            'CT/L': canonical_records.get('ТОННА ГРУЗОПОДЪЕМНОСТИ'),  # Carrying capacity in tonnes (code 36, if exists)
            '12U': canonical_records.get('ШТУКА'),  # 12 units (approximate to piece)
            'KG/NET EDA': canonical_records.get('КИЛОГРАММ'),  # Variant with / instead of space
            'KG MET.AM.': canonical_records.get('КИЛОГРАММ МЕТАЛЛИЧЕСКОГО АММИАКА'),  # Kilogram of metallic ammonium (if exists)
            'GI F/S': canonical_records.get('ГРАММ ДЕЛЯЩИХСЯ ИЗОТОПОВ'),  # Gram of fissile isotopes (code 38, if exists)
            'U (JEU/PACK)': canonical_records.get('УПАКОВКА') or canonical_records.get('ШТУКА'),  # Number of packages (code 10, if exists)
            'U JEU/PACK': canonical_records.get('УПАКОВКА') or canonical_records.get('ШТУКА'),  # Number of packages (without parentheses)
            'KG U': canonical_records.get('КИЛОГРАММ УРАНА'),  # Kilogram of uranium (code 35)
            'GT': canonical_records.get('ВАЛОВАЯ РЕГИСТРОВАЯ ВМЕСТИМОСТЬ'),  # Gross tonnage (code 40, if exists)
            'GRT': canonical_records.get('ВАЛОВАЯ РЕГИСТРОВАЯ ВМЕСТИМОСТЬ'),  # Gross register ton (code 39, if exists)

            # Other observed values from logs
            'KG NET EDA': canonical_records.get('КИЛОГРАММ'),
            'Л 100% СПИРТА': canonical_records.get('ЛИТР ЧИСТОГО (100%) СПИРТА'),
            'КГ NAOH': canonical_records.get('КИЛОГРАММ ГИДРОКСИДА НАТРИЯ'),
            'КГ KOH': canonical_records.get('КИЛОГРАММ ГИДРОКСИДА КАЛИЯ'),
            'КГ N': canonical_records.get('КИЛОГРАММ АЗОТА'),
            'КГ K2O': canonical_records.get('КИЛОГРАММ ОКСИДА КАЛИЯ'),
            'КГ P2O5': canonical_records.get('КИЛОГРАММ ПЯТИОКИСИ ФОСФОРА'),
            'КГ H2O2': canonical_records.get('КИЛОГРАММ ПЕРОКСИДА ВОДОРОДА'),
            'КГ 90 %-ГО СУХОГО ВЕЩЕСТВА': canonical_records.get('КИЛОГРАММ 90 %-ГО СУХОГО ВЕЩЕСТВА'),
            'КГ U': canonical_records.get('КИЛОГРАММ УРАНА'),
            
            # Additional variants and common misspellings
            'M³': canonical_records.get('КУБИЧЕСКИЙ МЕТР'),  # Superscript 3
            'M3': canonical_records.get('КУБИЧЕСКИЙ МЕТР'),  # Already exists, but ensure it's there
            'М³': canonical_records.get('КУБИЧЕСКИЙ МЕТР'),  # Cyrillic with superscript
            'KG H2O2': canonical_records.get('КИЛОГРАММ ПЕРОКСИДА ВОДОРОДА'),  # Latin version
            'KG N': canonical_records.get('КИЛОГРАММ АЗОТА'),  # Latin version
            'КГ 90% С/В': canonical_records.get('КИЛОГРАММ 90 %-ГО СУХОГО ВЕЩЕСТВА'),  # Variant with /В
            'КГ 90% СВ': canonical_records.get('КИЛОГРАММ 90 %-ГО СУХОГО ВЕЩЕСТВА'),  # Variant without /
            '1000 ШТ': canonical_records.get('ТЫСЯЧА ШТУК'),  # Russian version
            '100 ШТ': canonical_records.get('ШТУКА'),  # 100 pieces = pieces (approximate)
            'КАР': canonical_records.get('МЕТРИЧЕСКИЙ КАРАТ(1КАРАТ=2*10(-4)КГ'),  # Abbreviation
            'ЭЛЕМ': canonical_records.get('ШТУКА'),  # Element/piece (approximate)
            
            # Additional Comtrade abbreviations and variants
            'KG NAOH': canonical_records.get('КИЛОГРАММ ГИДРОКСИДА НАТРИЯ'),  # Sodium hydroxide
            'KG KOH': canonical_records.get('КИЛОГРАММ ГИДРОКСИДА КАЛИЯ'),  # Potassium hydroxide
            'KG K2O': canonical_records.get('КИЛОГРАММ ОКСИДА КАЛИЯ'),  # Potassium oxide
            'KG P2O5': canonical_records.get('КИЛОГРАММ ПЯТИОКИСИ ФОСФОРА'),  # Phosphorus pentoxide
            'KG 90% SDT': canonical_records.get('КИЛОГРАММ 90 %-ГО СУХОГО ВЕЩЕСТВА'),  # 90% dry substance (SDT variant)
            'KG 90% SD': canonical_records.get('КИЛОГРАММ 90 %-ГО СУХОГО ВЕЩЕСТВА'),  # 90% dry substance (SD variant)
            '1000 M3': canonical_records.get('1000 КУБИЧЕСКИХ МЕТРОВ'),  # 1000 cubic meters (if exists)
            'CE/EL': canonical_records.get('ЭЛЕМЕНТ') or canonical_records.get('ШТУКА'),  # Number of cells/elements
            'CE EL': canonical_records.get('ЭЛЕМЕНТ') or canonical_records.get('ШТУКА'),  # Number of cells/elements (space variant)
            'TJ': canonical_records.get('ТЕРАДЖОУЛЬ'),  # Terajoule (if exists)
            'TERAJOULE': canonical_records.get('ТЕРАДЖОУЛЬ'),  # Terajoule full name (if exists)
        }
        aliases.update(get_special_edizm_aliases(canonical_records))
        
        final_mapping.update(aliases)
        
        # Filter out any None values that may have resulted from missing keys
        final_mapping = {k: v for k, v in final_mapping.items() if v is not None and k is not None}

        # Diagnostic: Check if БЕККЕРЕЛЬ was loaded
        if 'БЕККЕРЕЛЬ' in canonical_records:
            logger.info(f"  - 'БЕККЕРЕЛЬ' found in canonical_records")
            if 'BQ' in final_mapping:
                logger.info(f"  - 'BQ' alias successfully added to final_mapping")
            else:
                logger.warning(f"  - 'BQ' alias NOT found in final_mapping (canonical_records.get returned None)")
        else:
            logger.warning(f"  - 'БЕККЕРЕЛЬ' NOT found in canonical_records")

        logger.info(f"Loaded common EDIZM mapping with {len(final_mapping)} case-insensitive keys.")
        return final_mapping
    except Exception as e:
        logger.error(f"Failed to load common EDIZM mapping: {e}")
        return {}

def load_strana_mapping(project_root: Path) -> Dict[str, str]:
    """Loads ISO2 to country name mapping from STRANA.csv."""
    mapping_file = project_root / "metadata" / "STRANA.csv"
    if not mapping_file.exists():
        logger.error(f"Country name mapping file not found at {mapping_file}")
        return {}
    
    try:
        # Assuming the separator is a tab.
        df = pd.read_csv(mapping_file, sep='	', dtype=str)
        df.columns = df.columns.str.upper()
        # Create case-insensitive mapping: uppercase KOD (ISO2) -> NAME
        mapping = pd.Series(df.NAME.values, index=df.KOD.str.upper()).to_dict()
        logger.info(f"Loaded country name mapping for {len(mapping)} countries.")
        return mapping
    except Exception as e:
        logger.error(f"Failed to load country name mapping: {e}")
        return {}

def load_tnved_mapping(project_root: Path) -> Dict[str, Dict[str, Dict[str, any]]]:
    """
    Loads TNVED code to name mappings from tnved.csv and missing_codes_translations.json.
    
    Returns a dictionary with structure:
    {
        'tnved2': {code: {'name': str, 'translated': bool}},
        'tnved4': {code: {'name': str, 'translated': bool}},
        ...
    }
    """
    mapping_file = project_root / "metadata" / "tnved.csv"
    translations_file = project_root / "metadata" / "translations" / "missing_codes_translations.json"
    
    # Initialize mappings structure
    mappings = {
        'tnved2': {},
        'tnved4': {},
        'tnved6': {},
        'tnved8': {},
        'tnved10': {}
    }
    
    # Load official mappings from tnved.csv
    if mapping_file.exists():
        try:
            df = pd.read_csv(mapping_file, dtype={'KOD': str, 'NAME': str, 'level': int})
            df.columns = df.columns.str.upper()

            for level in [2, 4, 6, 8, 10]:
                level_key = f'tnved{level}'
                level_data = df[df['LEVEL'] == level]
                for _, row in level_data.iterrows():
                    code = str(row['KOD']).strip()
                    name = str(row['NAME']).strip().upper()
                    mappings[level_key][code] = {
                        'name': name,
                        'translated': False
                    }
            
            logger.info("Successfully loaded official TNVED mappings for all levels.")
        except Exception as e:
            logger.error(f"Failed to load TNVED mapping from {mapping_file}: {e}")
    else:
        logger.warning(f"TNVED mapping file not found at {mapping_file}")
    
    # Load translations from missing_codes_translations_test.json
    if translations_file.exists():
        try:
            with open(translations_file, 'r', encoding='utf-8') as f:
                translations = json.load(f)
            
            translations_count = 0
            for code_10, data in translations.items():
                code_10_str = str(code_10).strip()
                russian_name = data.get('russian_name', '').strip().upper()
                
                if not russian_name:
                    continue
                
                # Pad code to 10 digits on the RIGHT if needed (never remove leading zeros)
                code_10_padded = code_10_str.strip()
                if len(code_10_padded) >= 10:
                    code_10_padded = code_10_padded[:10]
                else:
                    code_10_padded = code_10_padded + '0' * (10 - len(code_10_padded))
                
                # Add translation for level 10 (only if not already in official mappings)
                if code_10_padded not in mappings['tnved10']:
                    mappings['tnved10'][code_10_padded] = {
                        'name': russian_name,
                        'translated': True
                    }
                    translations_count += 1
                
                # Also add translations for parent levels (2, 4, 6, 8) if they don't exist
                # Extract parent codes from padded 10-digit code (preserving leading zeros)
                for level in [2, 4, 6, 8]:
                    level_key = f'tnved{level}'
                    # Extract first N digits from padded code
                    code_level = code_10_padded[:level]
                    
                    # Only add if this level code doesn't exist in official mappings
                    if code_level not in mappings[level_key]:
                        # Use the russian_name from the 10-digit code as fallback
                        # Note: This is not ideal, but we don't have separate translations for parent levels
                        mappings[level_key][code_level] = {
                            'name': russian_name,  # Using the 10-digit name as fallback
                            'translated': True
                        }
            
            if translations_count > 0:
                logger.info(f"Loaded {translations_count} translated TNVED codes from {translations_file}")
        except Exception as e:
            logger.error(f"Failed to load TNVED translations from {translations_file}: {e}")
    else:
        logger.warning(f"TNVED translations file not found at {translations_file}")
    
    return mappings

def load_and_transform_comtrade(
    comtrade_db_path: Path,
    project_root: Path,
    exclude_countries: List[str],
    start_year: int = None
) -> pd.DataFrame:
    """
    Loads and transforms Comtrade data, excluding specified countries.

    Args:
        comtrade_db_path: Path to the Comtrade DuckDB database.
        project_root: Path to the project root for metadata loading.
        exclude_countries: List of ISO2 country codes to exclude.

    Returns:
        A DataFrame with Comtrade data transformed to the unified schema.
    """
    logger.info(f"Loading Comtrade data, excluding: {exclude_countries}")
    
    partner_mapping = load_partner_mapping(project_root)
    if not partner_mapping:
        return pd.DataFrame()
    
    edizm_mapping = load_edizm_mapping(project_root)
    if not edizm_mapping:
        logger.error("Could not load Edizm mapping, aborting Comtrade load.")
        return pd.DataFrame()

    # Convert ISO2 country codes to Comtrade M49 codes for the query
    # Create case-insensitive mapping: uppercase ISO2 -> M49
    country_to_m49 = {v.upper(): k for k, v in partner_mapping.items() if v}
    
    # Convert exclude list to uppercase and map to M49
    exclude_countries_upper = [c.upper() for c in exclude_countries]
    exclude_m49_codes = []
    for c in exclude_countries_upper:
        if c in country_to_m49:
            m49_code = country_to_m49[c]
            exclude_m49_codes.append(m49_code)
            logger.info(f"Excluding country '{c}' (M49 code: {m49_code}) from Comtrade data")
        else:
            logger.warning(f"Could not find M49 code for country: {c}")
    
    logger.info(f"Total countries to exclude from Comtrade: {len(exclude_m49_codes)}")

    try:
        conn = duckdb.connect(str(comtrade_db_path), read_only=True)
        
        # Diagnostic: List tables and schema in the database
        tables = conn.execute("SHOW TABLES;").fetchall()
        logger.info(f"Tables found in {comtrade_db_path}: {tables}")
        try:
            table_info = conn.execute("DESCRIBE comtrade_data;").df()
            logger.info(f"Schema for comtrade_data:\n{table_info}")
        except Exception as e:
            logger.warning(f"Could not describe comtrade_data table: {e}")
        
        # Build query with safe formatting (M49 codes are always integers, so safe to format)
        # Try to include qtyCode if it exists in the table
        try:
            # Check if qtyCode column exists
            test_query = "SELECT qtyCode FROM comtrade_data LIMIT 1"
            conn.execute(test_query)
            has_qty_code = True
            logger.info("Found qtyCode column in comtrade_data table")
        except:
            has_qty_code = False
            logger.info("qtyCode column not found in comtrade_data table, using qtyUnitCode/altQtyUnitCode")
        
        query_parts = ["SELECT"]
        query_parts.append("    period AS PERIOD,")
        query_parts.append("    reporterCode AS STRANA_CODE,")
        query_parts.append("    cmdCode AS TNVED,")
        query_parts.append("    CASE flowCode WHEN 'M' THEN 'ЭК' WHEN 'X' THEN 'ИМ' WHEN 'ЭК' THEN 'ЭК' WHEN 'ИМ' THEN 'ИМ' END AS NAPR,")
        query_parts.append("    qtyUnitCode,")
        query_parts.append("    altQtyUnitCode,")
        if has_qty_code:
            query_parts.append("    qtyCode,")
        query_parts.append("    primaryValue AS STOIM,")
        query_parts.append("    netWgt AS NETTO,")
        query_parts.append("    qty,")
        query_parts.append("    altQty")
        query_parts.append("FROM comtrade_data")
        
        where_clauses = []
        
        if exclude_m49_codes:
            # M49 codes are always integers, safe to format directly
            # Validate all codes are integers for safety
            if not all(isinstance(code, int) for code in exclude_m49_codes):
                raise ValueError("All exclude_m49_codes must be integers")
            codes_str = ', '.join(map(str, exclude_m49_codes))
            where_clauses.append(f"reporterCode NOT IN ({codes_str})")
        
        if start_year:
            logger.info(f"Applying start_year filter to Comtrade data: year >= {start_year}")
            if not isinstance(start_year, int):
                raise ValueError("start_year must be an integer")
            where_clauses.append(f"refYear >= {start_year}")
        
        if where_clauses:
            query_parts.append("WHERE " + " AND ".join(where_clauses))
        
        query = "\n".join(query_parts)
        logger.info(f"Executing Comtrade query...")
        comtrade_df = conn.execute(query).fetchdf()
    except Exception as e:
        logger.error(f"Failed to query Comtrade data: {e}")
        return pd.DataFrame()
    finally:
        if 'conn' in locals():
            conn.close()
            
    if comtrade_df.empty:
        logger.info("Query returned no Comtrade data for the specified countries.")
        return pd.DataFrame()
    
    logger.info(f"Query returned {len(comtrade_df)} rows from Comtrade DB.")
        
    # Post-processing transformations
    logger.info("Transforming Comtrade data...")

    # Choose the supplementary quantity (KOL) and its unit code (EDIZM_CODE).
    # IMPORTANT: 
    # - qtyUnitCode is the PRIMARY unit CODE (usually weight in kg, code 8)
    # - qty is the PRIMARY quantity VALUES (weight values)
    # - altQtyUnitCode is the ALTERNATIVE/SUPPLEMENTARY unit CODE (what we need for EDIZM_CODE)
    # - altQty is the ALTERNATIVE/SUPPLEMENTARY quantity VALUES (what we need for KOL)
    # - qtyCode may exist as an additional source of unit CODE information
    #
    # Logic priority:
    # 1. Use altQtyUnitCode (CODE) and altQty (VALUES) if available - this is the supplementary unit we want
    # 2. If altQtyUnitCode is missing, check qtyCode (if it exists and is not kg/code 8) - use qtyCode for CODE and qty for VALUES
    # 3. Fallback to qtyUnitCode (CODE) and qty (VALUES) only if no alternative is available (but this is weight, not ideal)
    
    # Initialize EDIZM_CODE and KOL columns
    comtrade_df['EDIZM_CODE'] = None
    comtrade_df['KOL'] = None
    
    # Check if qtyCode column exists (may contain unit CODE information)
    has_qty_code = 'qtyCode' in comtrade_df.columns
    
    if 'altQtyUnitCode' in comtrade_df.columns and 'altQty' in comtrade_df.columns:
        # Priority 1: Use altQtyUnitCode (CODE) and altQty (VALUES) - this is the supplementary unit
        # altQtyUnitCode is the CODE of the supplementary unit
        # altQty is the VALUES in that supplementary unit
        # They can be independent - if we have the code, use it even if values are missing
        has_alt_code = comtrade_df['altQtyUnitCode'].notna()
        has_alt_values = comtrade_df['altQty'].notna()
        
        # Use altQtyUnitCode for EDIZM_CODE (unit code) if available
        comtrade_df.loc[has_alt_code, 'EDIZM_CODE'] = comtrade_df.loc[has_alt_code, 'altQtyUnitCode']
        # Use altQty for KOL (values) if available
        comtrade_df.loc[has_alt_values, 'KOL'] = comtrade_df.loc[has_alt_values, 'altQty']
        
        # Priority 2: If altQtyUnitCode is missing, check qtyCode (if available and not kg)
        # Use qtyCode for EDIZM_CODE (unit code) and qty for KOL (values)
        missing_alt_code = ~has_alt_code
        if has_qty_code:
            # Use qtyCode (CODE) if it exists, is not NULL, and is not 8 (kg)
            # Only use if EDIZM_CODE is still missing (altQtyUnitCode was not available)
            # Use qty (VALUES) for KOL if KOL is still missing
            qty_code_available = (
                missing_alt_code & 
                comtrade_df['qtyCode'].notna() & 
                (comtrade_df['qtyCode'] != 8)
            )
            comtrade_df.loc[qty_code_available, 'EDIZM_CODE'] = comtrade_df.loc[qty_code_available, 'qtyCode']
            
            # Use qty for KOL if KOL is still missing
            missing_kol = comtrade_df['KOL'].isna() & comtrade_df['qty'].notna()
            comtrade_df.loc[missing_kol, 'KOL'] = comtrade_df.loc[missing_kol, 'qty']
            missing_alt_code = missing_alt_code & ~qty_code_available
        
        # Priority 3: Fallback to qtyUnitCode (CODE) and qty (VALUES) - but this is weight, not ideal
        # Only use if EDIZM_CODE is still missing
        missing_edizm_code = comtrade_df['EDIZM_CODE'].isna()
        fallback_mask = missing_edizm_code & comtrade_df['qtyUnitCode'].notna()
        comtrade_df.loc[fallback_mask, 'EDIZM_CODE'] = comtrade_df.loc[fallback_mask, 'qtyUnitCode']
        
        # Use qty for KOL if KOL is still missing
        missing_kol = comtrade_df['KOL'].isna() & comtrade_df['qty'].notna()
        comtrade_df.loc[missing_kol, 'KOL'] = comtrade_df.loc[missing_kol, 'qty']
        
        # Log statistics
        logger.info(f"Unit mapping statistics:")
        logger.info(f"  - Using altQtyUnitCode (CODE): {has_alt_code.sum()}")
        logger.info(f"  - Using altQty (VALUES): {has_alt_values.sum()}")
        if has_qty_code:
            qty_code_used = missing_alt_code & comtrade_df['qtyCode'].notna() & (comtrade_df['qtyCode'] != 8)
            logger.info(f"  - Using qtyCode (CODE) where altQtyUnitCode missing: {qty_code_used.sum()}")
        logger.info(f"  - Using qtyUnitCode (CODE) as fallback: {fallback_mask.sum()}")
        logger.info(f"  - Using qty (VALUES) where altQty missing: {missing_kol.sum()}")
        logger.info(f"  - Missing EDIZM_CODE: {comtrade_df['EDIZM_CODE'].isna().sum()}")
        logger.info(f"  - Missing KOL: {comtrade_df['KOL'].isna().sum()}")
        
        # Diagnostic: Check for code 37 (Becquerels)
        code_37_mask = comtrade_df['altQtyUnitCode'] == 37
        if code_37_mask.any():
            logger.info(f"  - Found {code_37_mask.sum()} rows with altQtyUnitCode = 37 (Becquerels)")
            logger.info(f"  - Of these, {comtrade_df.loc[code_37_mask, 'EDIZM_CODE'].notna().sum()} have EDIZM_CODE set")
    else:
        logger.warning("altQtyUnitCode or altQty not found in Comtrade data.")
        # If altQtyUnitCode is not available, try qtyCode first
        if has_qty_code:
            # Use qtyCode (CODE) if it exists and is not 8 (kg)
            # Use qty (VALUES) for KOL
            qty_code_available = (
                comtrade_df['qtyCode'].notna() & 
                (comtrade_df['qtyCode'] != 8) &
                comtrade_df['qty'].notna()
            )
            comtrade_df.loc[qty_code_available, 'EDIZM_CODE'] = comtrade_df.loc[qty_code_available, 'qtyCode']
            comtrade_df.loc[qty_code_available, 'KOL'] = comtrade_df.loc[qty_code_available, 'qty']
            # For remaining rows, use qtyUnitCode (CODE) and qty (VALUES) as fallback
            remaining_mask = ~qty_code_available & comtrade_df['qty'].notna()
            comtrade_df.loc[remaining_mask, 'EDIZM_CODE'] = comtrade_df.loc[remaining_mask, 'qtyUnitCode']
            comtrade_df.loc[remaining_mask, 'KOL'] = comtrade_df.loc[remaining_mask, 'qty']
            logger.info(f"  - Using qtyCode (CODE) + qty (VALUES): {qty_code_available.sum()}")
            logger.info(f"  - Using qtyUnitCode (CODE) + qty (VALUES) as fallback: {remaining_mask.sum()}")
        else:
            # Last resort: use qtyUnitCode (CODE) and qty (VALUES) - but this is weight
            comtrade_df['EDIZM_CODE'] = comtrade_df['qtyUnitCode']
            comtrade_df['KOL'] = comtrade_df['qty']
            logger.warning("  - Using qtyUnitCode (CODE) + qty (VALUES) as last resort (this is weight, not ideal)")

    comtrade_df['STRANA'] = comtrade_df['STRANA_CODE'].map(partner_mapping)
    
    # Ensure STRANA is uppercase for consistency
    comtrade_df['STRANA'] = comtrade_df['STRANA'].str.upper()
    
    # Convert EDIZM_CODE to int for proper mapping (edizm_mapping uses int keys)
    # Handle NaN values - convert to int only for non-null values
    # First convert to float, then to Int64 to handle NaN properly
    comtrade_df['EDIZM_CODE_int'] = pd.to_numeric(comtrade_df['EDIZM_CODE'], errors='coerce').astype('Int64')
    
    # Map EDIZM_CODE (int) to EDIZM (string abbreviation like "Bq")
    comtrade_df['EDIZM'] = comtrade_df['EDIZM_CODE_int'].map(edizm_mapping)
    comtrade_df.fillna({'EDIZM': 'N/A'}, inplace=True)
    
    # Diagnostic: Check if code 37 was mapped to "Bq"
    code_37_mask = comtrade_df['EDIZM_CODE_int'] == 37
    if code_37_mask.any():
        code_37_count = code_37_mask.sum()
        code_37_edizm = comtrade_df.loc[code_37_mask, 'EDIZM'].unique()
        logger.info(f"  - Found {code_37_count} rows with EDIZM_CODE = 37 (Becquerels)")
        logger.info(f"  - EDIZM_CODE 37 mapped to EDIZM values: {code_37_edizm}")
        if 'Bq' in code_37_edizm or 'BQ' in code_37_edizm:
            bq_count = (comtrade_df.loc[code_37_mask, 'EDIZM'].isin(['Bq', 'BQ', 'bq'])).sum()
            logger.info(f"  - Of these, {bq_count} rows have EDIZM = 'Bq'")
    
    # Drop temporary column
    comtrade_df.drop(columns=['EDIZM_CODE_int'], inplace=True)
    
    null_strana_count = comtrade_df['STRANA'].isnull().sum()
    if null_strana_count > 0:
        logger.warning(f"Found {null_strana_count} rows with reporter codes that could not be mapped to ISO2 codes. These will be dropped.")
        unmapped_codes = comtrade_df[comtrade_df['STRANA'].isnull()]['STRANA_CODE'].unique()
        logger.warning(f"Unmapped reporter codes (sample): {unmapped_codes[:10]}")

    comtrade_df.dropna(subset=['STRANA'], inplace=True)
    logger.info(f"{len(comtrade_df)} rows remaining after dropping unmapped countries.")
    
    # Verify unique countries in Comtrade data
    comtrade_countries = comtrade_df['STRANA'].unique()
    logger.info(f"Countries in Comtrade data after transformation: {sorted(comtrade_countries)}")

    if comtrade_df.empty:
        logger.warning("No Comtrade data remaining after transformation.")
        return pd.DataFrame()
        
    comtrade_df['EDIZM_ISO'] = None

    comtrade_df = add_tnved_columns(comtrade_df)
    
    # Ensure data types match the expected schema
    for col, expected_type in EXPECTED_SCHEMA.items():
        if col in comtrade_df.columns and str(comtrade_df[col].dtype) != expected_type:
            try:
                if 'datetime' in expected_type:
                    if col == 'PERIOD':
                        # Convert to datetime and normalize to remove time component
                        comtrade_df[col] = pd.to_datetime(comtrade_df[col], errors='coerce').dt.normalize()
                    else:
                        comtrade_df[col] = pd.to_datetime(comtrade_df[col])
                else:
                    comtrade_df[col] = comtrade_df[col].astype(expected_type)
            except (ValueError, TypeError) as e:
                logger.warning(f"Could not convert Comtrade column '{col}' to {expected_type}: {e}")

    # Reorder columns to match the main schema
    final_cols = [col for col in EXPECTED_SCHEMA.keys() if col in comtrade_df.columns]
    return comtrade_df[final_cols]
    
def parse_merge_args(argv: List[str] = None):
    """Parse CLI arguments for the merge pipeline."""
    parser = argparse.ArgumentParser(
        description="Merge processed national data and optionally include Comtrade data."
    )
    parser.add_argument(
        '--include-comtrade',
        action='store_true',
        help="Include Comtrade data for countries not present in national data."
    )
    parser.add_argument(
        '--start-year',
        type=int,
        default=None,
        help="Filter all data to include records from this year onwards."
    )
    parser.add_argument(
        '--exclude-countries',
        type=str,
        nargs='+',
        default=[],
        help="List of ISO2 country codes to exclude from the merge (e.g., IN CN)."
    )
    parser.add_argument(
        '--output-db-path',
        type=str,
        default=None,
        help=(
            "Path to the output DuckDB file. Relative paths are resolved from "
            "the project root. Default: db/unified_trade_data.duckdb."
        ),
    )
    parser.add_argument(
        '--no-nowcast',
        dest='include_nowcast',
        action='store_false',
        help="Do not load nowcast from data_processed/nowcast/nowcast.parquet (TYPE=pred rows).",
    )
    parser.add_argument(
        '--no-fizob',
        dest='include_fizob',
        action='store_false',
        help="Do not load fizob_*.parquet files into the fizob_index table.",
    )
    parser.set_defaults(include_nowcast=True, include_fizob=True)
    return parser.parse_args(argv)


def resolve_merge_paths(project_root: Path = None, output_db_path: str = None) -> Dict[str, Path]:
    """Resolve project paths used by the merge pipeline."""
    if project_root is None:
        project_root = Path(__file__).resolve().parents[2]
    data_processed_dir = project_root / "data_processed"
    db_dir = project_root / "db"
    db_dir.mkdir(exist_ok=True)

    if output_db_path is None:
        resolved_output_db_path = db_dir / "unified_trade_data.duckdb"
    else:
        resolved_output_db_path = Path(output_db_path)
        if not resolved_output_db_path.is_absolute():
            resolved_output_db_path = project_root / resolved_output_db_path

    return {
        "project_root": project_root,
        "data_processed_dir": data_processed_dir,
        "db_dir": db_dir,
        "output_db_path": resolved_output_db_path,
        "comtrade_db_path": db_dir / "comtrade.db",
        "nowcast_path": data_processed_dir / "nowcast" / "nowcast.parquet",
    }


def discover_processed_files(data_processed_dir: Path):
    """Find regular and fizob parquet files in data_processed."""
    parquet_files = list(data_processed_dir.glob("*.parquet"))
    logger.info(f"Found {len(parquet_files)} parquet files: {[f.name for f in parquet_files]}")

    fizob_files = [f for f in parquet_files if f.name.startswith('fizob')]
    regular_files = [f for f in parquet_files if not f.name.startswith('fizob')]

    logger.info(f"Found {len(fizob_files)} fizob files: {[f.name for f in fizob_files]}")
    logger.info(f"Found {len(regular_files)} regular data files: {[f.name for f in regular_files]}")
    return regular_files, fizob_files


def load_national_datasets(
    regular_files: List[Path],
    excluded_countries_upper: List[str],
    start_year: int = None,
) -> Dict[str, pd.DataFrame]:
    """Load, validate and normalize processed national parquet datasets."""
    national_datasets = {}
    if not regular_files:
        logger.warning("No regular national parquet files found in data_processed directory.")
        return national_datasets

    for file_path in regular_files:
        country_code = file_path.stem.replace('_full', '').upper()
        if country_code in excluded_countries_upper:
            logger.info(f"Skipping {file_path.name} as per --exclude-countries argument.")
            continue

        df = load_and_validate_file(file_path, start_year=start_year)
        if df is not None:
            df_processed = generate_derived_columns(df)
            if 'STRANA' in df_processed.columns:
                df_processed['STRANA'] = df_processed['STRANA'].str.upper()
            national_datasets[country_code.lower()] = df_processed

    return national_datasets


def load_fizob_index_rows(fizob_files: List[Path], start_year: int = None) -> List[pd.DataFrame]:
    """Load fizob parquet files and transform them to unified fizob_index rows."""
    fizob_index_rows = []
    if not fizob_files:
        return fizob_index_rows

    logger.info("Loading fizob files for unified fizob_index table...")
    for file_path in fizob_files:
        file_stem = file_path.stem
        df = load_and_validate_file(file_path, start_year=start_year)
        if df is not None:
            df_processed = generate_derived_columns(df)
            if 'STRANA' in df_processed.columns:
                df_processed['STRANA'] = df_processed['STRANA'].str.upper()
            df_unified = transform_fizob_to_unified(df_processed, file_stem)
            if not df_unified.empty:
                fizob_index_rows.append(df_unified)
                logger.info(f"Loaded {file_stem}: {len(df_unified)} rows -> fizob_index")

    return fizob_index_rows


def append_national_data(
    all_dataframes: List[pd.DataFrame],
    national_datasets: Dict[str, pd.DataFrame],
) -> List[str]:
    """Append national datasets and return covered ISO country codes."""
    national_countries_iso = []
    for source_name, df in national_datasets.items():
        df['SOURCE'] = 'national'
        if 'TYPE' not in df.columns:
            df['TYPE'] = 'fact'
        else:
            df['TYPE'] = df['TYPE'].fillna('fact')
        all_dataframes.append(df)

        if 'STRANA' in df.columns and not df.empty:
            unique_countries = df['STRANA'].dropna().unique()
            for country in unique_countries:
                country_upper = country.upper()
                if country_upper not in national_countries_iso:
                    national_countries_iso.append(country_upper)

    return national_countries_iso


def append_comtrade_data(
    all_dataframes: List[pd.DataFrame],
    *,
    include_comtrade: bool,
    comtrade_db_path: Path,
    project_root: Path,
    national_countries_iso: List[str],
    excluded_countries_upper: List[str],
    start_year: int = None,
) -> None:
    """Optionally append transformed Comtrade data."""
    if not include_comtrade:
        return

    if not comtrade_db_path.exists():
        logger.error(f"Comtrade database not found at {comtrade_db_path}. Cannot include Comtrade data.")
        return

    countries_to_exclude_from_comtrade = list(set(national_countries_iso + excluded_countries_upper))
    logger.info(f"Excluding countries from Comtrade data to avoid duplicates: {countries_to_exclude_from_comtrade}")

    comtrade_df = load_and_transform_comtrade(
        comtrade_db_path,
        project_root,
        exclude_countries=countries_to_exclude_from_comtrade,
        start_year=start_year
    )
    if comtrade_df.empty:
        return

    initial_comtrade_rows = len(comtrade_df)
    indices_to_drop = comtrade_df[comtrade_df['STRANA'].isin(national_countries_iso)].index
    comtrade_df.drop(indices_to_drop, inplace=True)
    filtered_rows = initial_comtrade_rows - len(comtrade_df)
    if filtered_rows > 0:
        logger.info(f"Filtered {filtered_rows:,} duplicate rows from Comtrade data that matched national countries.")

    comtrade_df['SOURCE'] = 'comtrade'
    comtrade_df['TYPE'] = 'fact'
    all_dataframes.append(comtrade_df)


def _tnved_key_nowcast_overlap(value: object) -> str:
    """Canonical TNVED for (fact vs pred) cell join; must match normalize_tnved_code in the pipeline."""
    if pd.isna(value):
        return ''
    cleaned = re.sub(r"\.0$", "", str(value).strip()).strip()
    if not cleaned or cleaned.lower() == "nan":
        return ""
    return normalize_tnved_code(cleaned)


def drop_nowcast_rows_superseded_by_facts(
    merged_df: pd.DataFrame, logger_instance: logging.Logger
) -> pd.DataFrame:
    """Remove TYPE=pred (nowcast) rows when the same trade cell already has factual data.

    Join key: PERIOD (date), STRANA, TNVED (same normalization as normalized unified data:
    normalize_tnved_code — right-pad/truncate to 10, not left zfill), NAPR.
    """
    if merged_df.empty or 'TYPE' not in merged_df.columns:
        return merged_df

    type_norm = merged_df['TYPE'].astype(str).str.strip().str.lower()
    pred_mask = type_norm.eq('pred')
    if not pred_mask.any():
        return merged_df

    kp = pd.to_datetime(merged_df['PERIOD'], errors='coerce').dt.normalize()
    ks = merged_df['STRANA'].astype(str).str.strip().str.upper()
    kt = merged_df['TNVED'].map(_tnved_key_nowcast_overlap)
    kn = merged_df['NAPR'].astype(str).str.strip()

    fact_mask = (~pred_mask) & kp.notna()
    if not fact_mask.any():
        logger_instance.info(
            'No factual rows with valid PERIOD for nowcast supersession check; keeping all preds.'
        )
        return merged_df

    fact_keys = (
        pd.DataFrame({'_kp': kp[fact_mask], '_ks': ks[fact_mask], '_kt': kt[fact_mask], '_kn': kn[fact_mask]})
        .drop_duplicates()
    )

    pred_keys = pd.DataFrame(
        {
            '_row': merged_df.index[pred_mask],
            '_kp': kp[pred_mask].values,
            '_ks': ks[pred_mask].values,
            '_kt': kt[pred_mask].values,
            '_kn': kn[pred_mask].values,
        }
    )
    overlap = pred_keys.merge(fact_keys, on=['_kp', '_ks', '_kt', '_kn'], how='inner')
    drop_n = len(overlap)
    if drop_n == 0:
        logger_instance.info('Nowcast supersession: 0 pred rows removed (no overlap with factual keys).')
        return merged_df

    logger_instance.info(
        f'Dropped {drop_n:,} nowcast rows that duplicate factual '
        '(PERIOD, STRANA, TNVED, NAPR) cells.'
    )
    return merged_df.drop(index=overlap['_row'])


def append_nowcast_data(
    all_dataframes: List[pd.DataFrame],
    *,
    include_nowcast: bool,
    nowcast_path: Path,
    excluded_countries_upper: List[str],
    start_year: int = None,
) -> None:
    """Optionally append nowcast pred rows."""
    if not include_nowcast:
        logger.info("Nowcast disabled (--no-nowcast); not loading data_processed/nowcast/nowcast.parquet.")
        return

    if not nowcast_path.exists():
        logger.info(f"Nowcast file not found, skipping: {nowcast_path}")
        return

    logger.info(f"Loading nowcast data from {nowcast_path}")
    nowcast_raw_df = pd.read_parquet(nowcast_path)
    nowcast_df = transform_nowcast_to_unified(nowcast_raw_df, start_year=start_year)
    if nowcast_df.empty:
        return

    if excluded_countries_upper:
        before_excl = len(nowcast_df)
        nowcast_df = nowcast_df[~nowcast_df['STRANA'].isin(excluded_countries_upper)].copy()
        excluded_count = before_excl - len(nowcast_df)
        if excluded_count > 0:
            logger.info(f"Excluded {excluded_count:,} nowcast rows by --exclude-countries filter.")

    if not nowcast_df.empty:
        nowcast_df['SOURCE'] = 'nowcast'
        all_dataframes.append(nowcast_df)
        logger.info(f"Loaded nowcast rows (TYPE='pred'): {len(nowcast_df):,}")


def build_merged_dataframe(
    all_dataframes: List[pd.DataFrame],
    *,
    excluded_countries_upper: List[str],
    project_root: Path,
) -> pd.DataFrame:
    """Merge all sources and apply final shared normalization rules."""
    if not all_dataframes:
        logger.error("No data available to merge.")
        return pd.DataFrame()

    merged_df = pd.concat(all_dataframes, ignore_index=True)

    if excluded_countries_upper:
        initial_rows = len(merged_df)
        indices_to_drop = merged_df[merged_df['STRANA'].isin(excluded_countries_upper)].index
        merged_df.drop(indices_to_drop, inplace=True)
        excluded_rows = initial_rows - len(merged_df)
        if excluded_rows > 0:
            logger.info(f"Excluded {excluded_rows:,} rows for countries: {excluded_countries_upper}")

    merged_df = merged_df.sort_values(['PERIOD', 'STRANA', 'TNVED'])

    initial_rows = len(merged_df)
    merged_df.dropna(subset=['NAPR'], inplace=True)
    null_napr_rows = initial_rows - len(merged_df)
    if null_napr_rows > 0:
        logger.info(f"Removed {null_napr_rows:,} rows with NULL NAPR values")

    merged_df = drop_nowcast_rows_superseded_by_facts(merged_df, logger)

    logger.info("Standardizing EDIZM column...")
    common_edizm_map = load_common_edizm_mapping(project_root)
    if common_edizm_map:
        merged_df = standardize_edizm_columns(merged_df, common_edizm_map, logger)
    else:
        logger.error("Could not standardize EDIZM values due to mapping load failure.")

    return apply_special_edizm_cases(merged_df, logger)


def save_fizob_index(fizob_index_rows: List[pd.DataFrame], output_db_path: Path) -> None:
    """Save unified fizob_index table and computed view."""
    if not fizob_index_rows:
        return

    logger.info("Saving unified fizob_index table...")
    fizob_index_df = pd.concat(fizob_index_rows, ignore_index=True)
    conn = duckdb.connect(str(output_db_path))
    try:
        chunk_size = 100000
        first_chunk = fizob_index_df.iloc[:chunk_size]
        conn.register('fizob_chunk_df', first_chunk)
        conn.execute("""
            CREATE OR REPLACE TABLE fizob_index AS
            SELECT STRANA, NAPR, CAST(PERIOD AS DATE) AS PERIOD, tn_level, tn_code, fizob, fizob_bp
            FROM fizob_chunk_df
        """)
        conn.unregister('fizob_chunk_df')
        logger.info(f"  ... created fizob_index and inserted first {len(first_chunk):,} rows")

        for i in range(chunk_size, len(fizob_index_df), chunk_size):
            chunk = fizob_index_df.iloc[i:i + chunk_size]
            conn.register('fizob_chunk_df', chunk)
            conn.execute("""
                INSERT INTO fizob_index
                SELECT STRANA, NAPR, CAST(PERIOD AS DATE) AS PERIOD, tn_level, tn_code, fizob, fizob_bp
                FROM fizob_chunk_df
            """)
            conn.unregister('fizob_chunk_df')
            logger.info(f"  ... inserted {i + len(chunk):,} / {len(fizob_index_df):,} rows")

        result = conn.execute("SELECT COUNT(*) FROM fizob_index").fetchone()
        logger.info(f"  ... saved {result[0]:,} rows to fizob_index")

        conn.execute("""
            CREATE OR REPLACE VIEW fizob_index_v AS
            SELECT *,
                   CASE WHEN fizob_bp = 0 THEN NULL ELSE fizob / fizob_bp END AS idx
            FROM fizob_index
        """)
        logger.info("  ... created view fizob_index_v with computed idx column")
    except Exception as e:
        logger.error(f"Failed to save fizob_index: {e}")
        raise
    finally:
        conn.close()


def create_reference_tables(output_db_path: Path, project_root: Path) -> None:
    """Create DuckDB reference tables and convenience views."""
    conn = duckdb.connect(str(output_db_path))
    try:
        save_reference_tables(conn, project_root)
    except Exception as e:
        logger.error(f"Failed to create reference tables: {e}")
        raise
    finally:
        conn.close()


def log_merge_summary(merged_df: pd.DataFrame) -> None:
    """Log final merge and fact/pred coverage summary."""
    logger.info("=== MERGE SUMMARY ===")
    logger.info(f"Total rows: {len(merged_df)}")
    logger.info(f"Unique countries: {merged_df['STRANA'].nunique()}")
    logger.info(f"Date range: {merged_df['PERIOD'].min()} to {merged_df['PERIOD'].max()}")

    logger.info("Rows by source:")
    source_counts = merged_df['SOURCE'].value_counts()
    for source, count in source_counts.items():
        logger.info(f"  {source}: {count:,} rows")

    logger.info("=== SANITY CHECK: FACT VS PRED ===")
    if 'TYPE' in merged_df.columns:
        merged_df['TYPE'] = merged_df['TYPE'].fillna('fact')
        type_counts = merged_df['TYPE'].value_counts(dropna=False)
        total_rows = len(merged_df)
        for type_value, count in type_counts.items():
            share = (count / total_rows * 100) if total_rows > 0 else 0
            logger.info(f"  TYPE={type_value}: {count:,} rows ({share:.2f}%)")

        pred_df = merged_df[merged_df['TYPE'] == 'pred'].copy()
        if pred_df.empty:
            logger.info("  No TYPE='pred' rows found in merged dataset.")
        else:
            logger.info(f"  Pred date range: {pred_df['PERIOD'].min()} to {pred_df['PERIOD'].max()}")
            pred_month_counts = (
                pred_df
                .groupby(pred_df['PERIOD'].dt.to_period('M'))
                .size()
                .sort_index()
            )
            logger.info("  Pred rows by month:")
            for period_month, count in pred_month_counts.items():
                logger.info(f"    {period_month}: {count:,}")

            pred_country_counts = pred_df.groupby('STRANA').size().sort_values(ascending=False)
            logger.info("  Pred rows by country:")
            for country, count in pred_country_counts.items():
                logger.info(f"    {country}: {count:,}")

            pred_country_month = (
                pred_df.assign(PERIOD_MONTH=pred_df['PERIOD'].dt.to_period('M').astype(str))
                .groupby(['STRANA', 'PERIOD_MONTH'])
                .size()
                .reset_index(name='count')
                .sort_values(['STRANA', 'PERIOD_MONTH'])
            )
            logger.info("  Pred coverage by country and month:")
            for _, row in pred_country_month.iterrows():
                logger.info(f"    {row['STRANA']} | {row['PERIOD_MONTH']}: {row['count']:,}")
    else:
        logger.warning("TYPE column not found: sanity-check for fact/pred skipped.")

    logger.info("Rows by country:")
    logger.info(str(merged_df.groupby('SOURCE')['STRANA'].value_counts()))

    logger.info("EDIZM counts by country:")
    edizm_counts = merged_df.groupby(['STRANA', 'EDIZM']).size().reset_index(name='count')
    edizm_counts = edizm_counts.sort_values(['STRANA', 'count'], ascending=[True, False])
    for strana, group in edizm_counts.groupby('STRANA'):
        logger.info(f"  Country: {strana}")
        for _, row in group.head(5).iterrows():
            logger.info(f"    - {row['EDIZM']}: {row['count']:,} rows")


def run_merge_pipeline(args, paths: Dict[str, Path]) -> None:
    """Run the merge pipeline stages in order."""
    logger.info("Starting data merging process...")

    excluded_countries_upper = [c.upper() for c in args.exclude_countries]
    regular_files, fizob_files = discover_processed_files(paths["data_processed_dir"])
    national_datasets = load_national_datasets(
        regular_files,
        excluded_countries_upper,
        start_year=args.start_year,
    )
    if args.include_fizob:
        fizob_index_rows = load_fizob_index_rows(fizob_files, start_year=args.start_year)
    else:
        logger.info("Fizob disabled (--no-fizob); not loading fizob_*.parquet into fizob_index.")
        fizob_index_rows = []

    all_dataframes = []
    national_countries_iso = append_national_data(all_dataframes, national_datasets)
    append_comtrade_data(
        all_dataframes,
        include_comtrade=args.include_comtrade,
        comtrade_db_path=paths["comtrade_db_path"],
        project_root=paths["project_root"],
        national_countries_iso=national_countries_iso,
        excluded_countries_upper=excluded_countries_upper,
        start_year=args.start_year,
    )
    append_nowcast_data(
        all_dataframes,
        include_nowcast=args.include_nowcast,
        nowcast_path=paths["nowcast_path"],
        excluded_countries_upper=excluded_countries_upper,
        start_year=args.start_year,
    )

    merged_df = build_merged_dataframe(
        all_dataframes,
        excluded_countries_upper=excluded_countries_upper,
        project_root=paths["project_root"],
    )
    if merged_df.empty:
        return

    logger.info("Reference tables (country names, TNVED names) will be created as separate tables in the database.")
    if not smoke_check_merged_dataset(merged_df):
        logger.error("Aborting save: smoke checks failed. The existing DuckDB was NOT modified.")
        return

    save_to_duckdb(merged_df, paths["output_db_path"])
    save_fizob_index(fizob_index_rows, paths["output_db_path"])
    create_reference_tables(paths["output_db_path"], paths["project_root"])

    logger.info("Data merge completed. To process outliers, run: python src/outlier_detection.py")
    log_merge_summary(merged_df)


def main(argv: List[str] = None):
    """CLI orchestration layer for the merge pipeline."""
    args = parse_merge_args(argv)
    paths = resolve_merge_paths(output_db_path=args.output_db_path)
    run_merge_pipeline(args, paths)

if __name__ == "__main__":
    main()
