"""Shared helpers for country collectors and processors.

Keeps argparse year validation, project-root resolution and logging setup in
one place instead of copy-pasting them into every collector.
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime
from pathlib import Path

MIN_DATA_YEAR = 2005

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"


def get_project_root() -> Path:
    """Repo root, resolved from src/collectors/ location."""
    return Path(__file__).resolve().parents[2]


def setup_logging(name: str) -> logging.Logger:
    """Configure the shared collector logging format and return a logger."""
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    return logging.getLogger(name)


def valid_year(value: str, min_year: int = MIN_DATA_YEAR) -> str:
    """argparse type: a year between min_year and the current year (as string)."""
    current_year = datetime.now().year
    if not str(value).isdigit():
        raise argparse.ArgumentTypeError(
            f"Year should be a number in range {min_year}-{current_year}"
        )
    year = int(value)
    if year < min_year or year > current_year:
        raise argparse.ArgumentTypeError(
            f"Year should be a number in range {min_year}-{current_year}"
        )
    return str(value)


__all__ = ["LOG_FORMAT", "MIN_DATA_YEAR", "get_project_root", "setup_logging", "valid_year"]
