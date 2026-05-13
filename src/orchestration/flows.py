"""Prefect flows for the MGIMO foreign trade data pipeline.

This module intentionally starts as a thin orchestration layer over the current
CLI/R scripts. The business logic stays in the existing processors and
pipelines; Prefect owns sequencing, logs, retries, and run parameters.
"""

from __future__ import annotations

import json
import logging
import os
import queue
import shutil
import subprocess
import sys
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from prefect import flow, get_run_logger, task

try:
    from .checks import run_sql_quality_checks
except ImportError:  # pragma: no cover - supports running this file directly
    from checks import run_sql_quality_checks


PROJECT_ROOT = Path(__file__).resolve().parents[2]
NOWCAST_R_PACKAGES = ("tidyverse", "duckdb", "dfms", "arrow", "vars")
FIZOB_R_PACKAGES = ("tidyverse", "slider", "duckdb")


def _command_text(command: Sequence[str]) -> str:
    """Render a command for logs without invoking a shell."""
    return " ".join(str(part) for part in command)


def _build_merge_command(
    python: str,
    *,
    include_comtrade: bool,
    include_nowcast: bool,
    include_fizob: bool,
    start_year: int | None,
    output_db_path: str | None,
) -> list[str]:
    """Build the current merge CLI command."""
    command = [python, "src/merge_processed_data.py"]
    if include_comtrade:
        command.append("--include-comtrade")
    if not include_nowcast:
        command.append("--no-nowcast")
    if not include_fizob:
        command.append("--no-fizob")
    if start_year is not None:
        command.extend(["--start-year", str(start_year)])
    if output_db_path is not None:
        command.extend(["--output-db-path", output_db_path])
    return command


def _build_r_package_check_command(rscript: str, packages: Sequence[str]) -> list[str]:
    """Build an R command that fails with a clear missing-package list."""
    package_vector = ", ".join(f'"{package}"' for package in sorted(set(packages)))
    expression = (
        f"missing <- setdiff(c({package_vector}), rownames(installed.packages())); "
        'if (length(missing)) stop("Missing R packages: ", '
        'paste(missing, collapse = ", "), call. = FALSE)'
    )
    return [rscript, "-e", expression]


def _build_r_parse_check_command(rscript: str, script_path: str) -> list[str]:
    """Build an R command that validates a script without executing it."""
    escaped_path = script_path.replace("\\", "/").replace("'", "\\'")
    expression = f"parse(file = '{escaped_path}'); cat('R parse OK: {escaped_path}\\n')"
    return [rscript, "-e", expression]


def _resolve_db_path(project_root: str, output_db_path: str | None) -> str:
    """Resolve the DuckDB path that should be checked after merge."""
    root = Path(project_root)
    if output_db_path is None:
        return str(root / "db" / "unified_trade_data.duckdb")
    path = Path(output_db_path)
    if not path.is_absolute():
        path = root / path
    return str(path)


def _resolve_executable(executable: str) -> str:
    """Resolve an executable name to a runnable path when possible."""
    path = Path(executable)
    if path.parent != Path("."):
        return str(path)

    resolved = shutil.which(executable) or shutil.which(f"{executable}.exe")
    if resolved is None:
        raise FileNotFoundError(
            f"Executable not found: {executable}. "
            "Pass an absolute path or add it to PATH."
        )
    return resolved


def _relative_to_root(path: Path, root: Path) -> str:
    """Return a stable project-relative path when possible."""
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def _file_version(path: Path, root: Path) -> dict[str, Any]:
    """Capture a cheap, stable file fingerprint for run manifests."""
    resolved = path if path.is_absolute() else root / path
    metadata: dict[str, Any] = {
        "path": _relative_to_root(resolved, root),
        "exists": resolved.exists(),
    }
    if not resolved.exists():
        return metadata

    stat = resolved.stat()
    metadata.update(
        {
            "size_bytes": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
            "mtime_utc": datetime.fromtimestamp(stat.st_mtime, UTC).isoformat(),
            "fingerprint": f"{stat.st_size}:{stat.st_mtime_ns}",
        }
    )
    return metadata


def _discover_input_files(
    root: Path,
    *,
    nowcast_output_dir: str,
    fizob_output_dir: str,
) -> list[Path]:
    """List current file inputs that feed the merge/orchestration layer."""
    paths: set[Path] = set()

    data_processed = root / "data_processed"
    if data_processed.exists():
        paths.update(data_processed.glob("*.parquet"))

    nowcast_dir = Path(nowcast_output_dir)
    if not nowcast_dir.is_absolute():
        nowcast_dir = root / nowcast_dir
    if nowcast_dir.exists():
        paths.update(nowcast_dir.glob("*.parquet"))

    fizob_dir = Path(fizob_output_dir)
    if not fizob_dir.is_absolute():
        fizob_dir = root / fizob_dir
    if fizob_dir.exists():
        paths.update(fizob_dir.glob("fizob_*.parquet"))

    for db_file in ("db/comtrade.db",):
        path = root / db_file
        if path.exists():
            paths.add(path)

    return sorted(paths, key=lambda path: _relative_to_root(path, root))


def _git_metadata(root: Path) -> dict[str, Any]:
    """Collect best-effort git metadata for reproducibility."""
    metadata: dict[str, Any] = {}
    commands = {
        "commit": ["git", "rev-parse", "HEAD"],
        "branch": ["git", "branch", "--show-current"],
        "status_short": ["git", "status", "--short"],
    }
    for key, command in commands.items():
        completed = subprocess.run(
            command,
            cwd=root,
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            check=False,
        )
        if completed.returncode == 0:
            value = completed.stdout.strip()
            metadata[key] = value.splitlines() if key == "status_short" else value
        else:
            metadata[key] = None
    return metadata


def _prefect_or_module_logger() -> Any:
    """Use Prefect run logs when available, otherwise a regular logger."""
    try:
        return get_run_logger()
    except Exception:
        return logging.getLogger(__name__)


def _enqueue_pipe_lines(
    pipe: Any,
    stream_name: str,
    output_queue: "queue.Queue[tuple[str, str]]",
) -> None:
    """Read process output in a background thread to avoid pipe deadlocks."""
    try:
        for line in iter(pipe.readline, ""):
            output_queue.put((stream_name, line.rstrip()))
    finally:
        pipe.close()


@task(name="run-command", retries=1, retry_delay_seconds=30)
def run_command(
    command: Sequence[str],
    *,
    project_root: str | None = None,
    env: Mapping[str, str] | None = None,
) -> None:
    """Run one existing project command and fail the task on non-zero exit."""
    logger = _prefect_or_module_logger()
    cwd = Path(project_root) if project_root else PROJECT_ROOT

    merged_env = os.environ.copy()
    merged_env.setdefault("PYTHONIOENCODING", "utf-8")
    merged_env.setdefault("PYTHONUNBUFFERED", "1")
    if env:
        merged_env.update({key: str(value) for key, value in env.items()})

    resolved_command = [
        _resolve_executable(str(command[0])),
        *[str(part) for part in command[1:]],
    ]

    logger.info("Running: %s", _command_text(resolved_command))
    process = subprocess.Popen(
        resolved_command,
        cwd=cwd,
        env=merged_env,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    output_queue: "queue.Queue[tuple[str, str]]" = queue.Queue()
    stdout_thread = threading.Thread(
        target=_enqueue_pipe_lines,
        args=(process.stdout, "stdout", output_queue),
        daemon=True,
    )
    stderr_thread = threading.Thread(
        target=_enqueue_pipe_lines,
        args=(process.stderr, "stderr", output_queue),
        daemon=True,
    )
    stdout_thread.start()
    stderr_thread.start()

    stderr_lines: list[str] = []
    while stdout_thread.is_alive() or stderr_thread.is_alive() or not output_queue.empty():
        try:
            stream_name, line = output_queue.get(timeout=0.2)
        except queue.Empty:
            continue
        if not line:
            continue
        if stream_name == "stderr":
            stderr_lines.append(line)
        logger.info(line)

    returncode = process.wait()
    stdout_thread.join(timeout=1)
    stderr_thread.join(timeout=1)

    if returncode != 0:
        for line in stderr_lines:
            logger.warning(line)
        raise RuntimeError(
            f"Command failed with exit code {returncode}: "
            f"{_command_text(resolved_command)}"
        )


@task(name="sql-quality-checks")
def run_sql_quality_checks_task(db_path: str, *, require_fizob: bool = False) -> dict[str, Any]:
    """Run SQL quality checks as a Prefect task."""
    logger = _prefect_or_module_logger()
    metrics = run_sql_quality_checks(db_path, require_fizob=require_fizob)
    logger.info("SQL quality checks passed for %s", db_path)
    logger.info("Quality metrics: %s", metrics)
    return metrics


@task(name="write-run-manifest")
def write_run_manifest_task(
    *,
    project_root: str,
    parameters: Mapping[str, Any],
    final_db_path: str,
    quality_metrics: Mapping[str, Any] | None,
    manifest_dir: str,
    nowcast_output_dir: str,
    fizob_output_dir: str,
) -> str:
    """Write a JSON manifest describing the completed orchestration run."""
    logger = _prefect_or_module_logger()
    root = Path(project_root)
    run_finished_at = datetime.now(UTC)

    manifest_root = Path(manifest_dir)
    if not manifest_root.is_absolute():
        manifest_root = root / manifest_root
    manifest_root.mkdir(parents=True, exist_ok=True)

    input_files = _discover_input_files(
        root,
        nowcast_output_dir=nowcast_output_dir,
        fizob_output_dir=fizob_output_dir,
    )
    manifest = {
        "flow": "mgimo-full-refresh",
        "run_finished_at_utc": run_finished_at.isoformat(),
        "project_root": str(root.resolve()),
        "git": _git_metadata(root),
        "parameters": dict(parameters),
        "artifacts": {
            "final_db": _file_version(Path(final_db_path), root),
            "nowcast_output_dir": str(nowcast_output_dir),
            "fizob_output_dir": str(fizob_output_dir),
        },
        "input_files": [_file_version(path, root) for path in input_files],
        "quality_checks": {
            "enabled": quality_metrics is not None,
            "metrics": dict(quality_metrics) if quality_metrics is not None else None,
        },
    }

    manifest_name = f"mgimo_full_refresh_{run_finished_at.strftime('%Y%m%dT%H%M%SZ')}.json"
    manifest_path = manifest_root / manifest_name
    latest_path = manifest_root / "latest.json"
    manifest_json = json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True)
    manifest_path.write_text(manifest_json + "\n", encoding="utf-8")
    latest_path.write_text(manifest_json + "\n", encoding="utf-8")
    logger.info("Run manifest saved to %s", manifest_path)
    logger.info("Latest manifest updated at %s", latest_path)
    return str(manifest_path)


@flow(name="mgimo-full-refresh")
def mgimo_full_refresh(
    *,
    process_china: bool = False,
    process_india: bool = False,
    process_turkey: bool = False,
    include_comtrade: bool = True,
    include_nowcast_in_merge: bool = True,
    run_nowcast: bool = False,
    run_fizob: bool = False,
    run_quality_checks: bool = True,
    require_fizob_quality: bool = False,
    run_outlier_detection: bool = False,
    start_year: int | None = 2019,
    output_db_path: str | None = None,
    nowcast_output_dir: str = "data_processed/nowcast",
    fizob_output_dir: str = "data_processed",
    write_manifest: bool = True,
    manifest_dir: str = "data_processed/manifests",
    rscript: str = "Rscript",
    project_root: str | None = None,
) -> None:
    """Run the current end-to-end refresh using existing scripts.

    Merge and quality checks are enabled by default. Country processing and
    R-derived steps are opt-in because they are heavier and should be explicit
    in routine refreshes.
    """
    root = str(Path(project_root) if project_root else PROJECT_ROOT)
    python = sys.executable
    final_db_path = _resolve_db_path(root, output_db_path)
    flow_parameters = {
        "process_china": process_china,
        "process_india": process_india,
        "process_turkey": process_turkey,
        "include_comtrade": include_comtrade,
        "include_nowcast_in_merge": include_nowcast_in_merge,
        "run_nowcast": run_nowcast,
        "run_fizob": run_fizob,
        "run_quality_checks": run_quality_checks,
        "require_fizob_quality": require_fizob_quality,
        "run_outlier_detection": run_outlier_detection,
        "start_year": start_year,
        "output_db_path": output_db_path,
        "nowcast_output_dir": nowcast_output_dir,
        "fizob_output_dir": fizob_output_dir,
        "write_manifest": write_manifest,
        "manifest_dir": manifest_dir,
        "rscript": rscript,
        "project_root": project_root,
    }

    required_r_packages: set[str] = set()
    if run_nowcast:
        required_r_packages.update(NOWCAST_R_PACKAGES)
    if run_fizob:
        required_r_packages.update(FIZOB_R_PACKAGES)
    if required_r_packages:
        run_command(
            _build_r_package_check_command(rscript, sorted(required_r_packages)),
            project_root=root,
        )
    if run_nowcast:
        run_command(
            _build_r_parse_check_command(rscript, "src/nowcast.R"),
            project_root=root,
        )
    if run_fizob:
        run_command(
            _build_r_parse_check_command(rscript, "src/fizob_queries.R"),
            project_root=root,
        )

    if process_china:
        run_command([python, "src/collectors/china_processor.py"], project_root=root)

    if process_india:
        run_command([python, "src/collectors/india_processor.py"], project_root=root)

    if process_turkey:
        run_command([python, "src/collectors/turkey_processor.py", "--all"], project_root=root)

    # When nowcast is recomputed, build a fact-only base first so the R script
    # does not depend on a stale nowcast parquet from a previous run.
    initial_include_nowcast = include_nowcast_in_merge and not run_nowcast
    initial_include_fizob = not run_fizob
    run_command(
        _build_merge_command(
            python,
            include_comtrade=include_comtrade,
            include_nowcast=initial_include_nowcast,
            include_fizob=initial_include_fizob,
            start_year=start_year,
            output_db_path=output_db_path,
        ),
        project_root=root,
    )

    derived_steps_ran = False
    if run_nowcast:
        run_command(
            [
                rscript,
                "src/nowcast.R",
                "--db-path",
                final_db_path,
                "--output-dir",
                nowcast_output_dir,
            ],
            project_root=root,
        )
        derived_steps_ran = True

    if run_fizob:
        run_command(
            [
                rscript,
                "src/fizob_queries.R",
                "--db-path",
                final_db_path,
                "--output-dir",
                fizob_output_dir,
            ],
            project_root=root,
        )
        derived_steps_ran = True

    # Current R scripts publish derived results as parquet files. Re-run merge
    # to absorb fresh nowcast/fizob artifacts into the DuckDB file until these
    # derived tables move into the builder itself.
    if derived_steps_ran:
        run_command(
            _build_merge_command(
                python,
                include_comtrade=include_comtrade,
                include_nowcast=include_nowcast_in_merge,
                include_fizob=True,
                start_year=start_year,
                output_db_path=output_db_path,
            ),
            project_root=root,
        )

    quality_metrics: dict[str, Any] | None = None
    if run_quality_checks:
        quality_metrics = run_sql_quality_checks_task(
            final_db_path,
            require_fizob=require_fizob_quality,
        )

    if run_outlier_detection:
        run_command([python, "src/outlier_detection.py"], project_root=root)

    if write_manifest:
        write_run_manifest_task(
            project_root=root,
            parameters=flow_parameters,
            final_db_path=final_db_path,
            quality_metrics=quality_metrics,
            manifest_dir=manifest_dir,
            nowcast_output_dir=nowcast_output_dir,
            fizob_output_dir=fizob_output_dir,
        )


if __name__ == "__main__":
    mgimo_full_refresh()
