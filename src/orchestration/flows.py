"""Prefect flows for the MGIMO foreign trade data pipeline.

This module intentionally starts as a thin orchestration layer over the current
CLI/R scripts. The business logic stays in the existing processors and
pipelines; Prefect owns sequencing, logs, retries, and run parameters.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Mapping, Sequence

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
    start_year: int | None,
    output_db_path: str | None,
) -> list[str]:
    """Build the current merge CLI command."""
    command = [python, "src/merge_processed_data.py"]
    if include_comtrade:
        command.append("--include-comtrade")
    if not include_nowcast:
        command.append("--no-nowcast")
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


@task(name="run-command", retries=1, retry_delay_seconds=30)
def run_command(
    command: Sequence[str],
    *,
    project_root: str | None = None,
    env: Mapping[str, str] | None = None,
) -> None:
    """Run one existing project command and fail the task on non-zero exit."""
    logger = get_run_logger()
    cwd = Path(project_root) if project_root else PROJECT_ROOT

    merged_env = os.environ.copy()
    merged_env.setdefault("PYTHONIOENCODING", "utf-8")
    if env:
        merged_env.update({key: str(value) for key, value in env.items()})

    resolved_command = [
        _resolve_executable(str(command[0])),
        *[str(part) for part in command[1:]],
    ]

    logger.info("Running: %s", _command_text(resolved_command))
    completed = subprocess.run(
        resolved_command,
        cwd=cwd,
        env=merged_env,
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        check=False,
    )

    if completed.stdout:
        logger.info(completed.stdout.strip())
    if completed.stderr:
        log_stderr = logger.warning if completed.returncode != 0 else logger.info
        log_stderr(completed.stderr.strip())

    if completed.returncode != 0:
        raise RuntimeError(
            f"Command failed with exit code {completed.returncode}: "
            f"{_command_text(resolved_command)}"
        )


@task(name="sql-quality-checks")
def run_sql_quality_checks_task(db_path: str, *, require_fizob: bool = False) -> None:
    """Run SQL quality checks as a Prefect task."""
    logger = get_run_logger()
    metrics = run_sql_quality_checks(db_path, require_fizob=require_fizob)
    logger.info("SQL quality checks passed for %s", db_path)
    logger.info("Quality metrics: %s", metrics)


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

    if process_china:
        run_command([python, "src/collectors/china_processor.py"], project_root=root)

    if process_india:
        run_command([python, "src/collectors/india_processor.py"], project_root=root)

    if process_turkey:
        run_command([python, "src/collectors/turkey_processor.py", "--all"], project_root=root)

    # When nowcast is recomputed, build a fact-only base first so the R script
    # does not depend on a stale nowcast parquet from a previous run.
    initial_include_nowcast = include_nowcast_in_merge and not run_nowcast
    run_command(
        _build_merge_command(
            python,
            include_comtrade=include_comtrade,
            include_nowcast=initial_include_nowcast,
            start_year=start_year,
            output_db_path=output_db_path,
        ),
        project_root=root,
    )

    derived_steps_ran = False
    if run_nowcast:
        run_command([rscript, "src/nowcast.R"], project_root=root)
        derived_steps_ran = True

    if run_fizob:
        run_command([rscript, "src/fizob_queries.R"], project_root=root)
        derived_steps_ran = True

    # Current R scripts publish derived results as parquet files in
    # data_processed/. Re-run merge to absorb fresh nowcast/fizob artifacts into
    # the DuckDB file until these derived tables move into the builder itself.
    if derived_steps_ran:
        run_command(
            _build_merge_command(
                python,
                include_comtrade=include_comtrade,
                include_nowcast=include_nowcast_in_merge,
                start_year=start_year,
                output_db_path=output_db_path,
            ),
            project_root=root,
        )

    if run_quality_checks:
        run_sql_quality_checks_task(final_db_path, require_fizob=require_fizob_quality)

    if run_outlier_detection:
        run_command([python, "src/outlier_detection.py"], project_root=root)


if __name__ == "__main__":
    mgimo_full_refresh()
