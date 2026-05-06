#!/usr/bin/env python3
"""Shared pytest configuration for stable local test runs on Windows/YandexDisk."""

import os
import uuid
from pathlib import Path


def pytest_configure(config):
    """Keep pytest temp files inside the workspace instead of locked system temp."""
    repo_root = Path(__file__).resolve().parents[1]
    temp_root = repo_root / ".pytest_tmp"
    session_temp = temp_root / f"run-{os.getpid()}-{uuid.uuid4().hex}"

    temp_root.mkdir(exist_ok=True)

    if config.option.basetemp is None:
        config.option.basetemp = str(session_temp)
