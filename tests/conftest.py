#!/usr/bin/env python3
"""Shared pytest configuration for stable local test runs on Windows/YandexDisk."""

import os
import uuid
from pathlib import Path
import pytest


@pytest.fixture
def reference_fixture(tmp_path):
    """Small source reference plus versioned overrides; no private local CSVs."""
    import shutil
    import csv
    root=Path(__file__).resolve().parents[1]
    metadata=tmp_path/'metadata'
    (metadata/'translations').mkdir(parents=True)
    for name in ('name_overrides.json','country_chapters.json'):
        shutil.copy2(root/'metadata/translations'/name,metadata/'translations'/name)
    rows=[('98',2,'СТАРОЕ НАЗВАНИЕ'),('8703401010',10,'АВТОМОБИЛИ ДО 1000 СМ3'),('870322',6,'БЕНЗИНОВЫЕ АВТОМОБИЛИ')]
    for code in ('870340','870350','870360','870370'):
        rows.extend((code.ljust(level,'0'),level,'СТАРОЕ НАЗВАНИЕ') for level in (6,8,10))
    with (metadata/'tnved.csv').open('w',encoding='utf-8',newline='') as f:
        writer=csv.writer(f);writer.writerow(['KOD','level','NAME']);writer.writerows(rows)
    return tmp_path


def pytest_configure(config):
    """Keep pytest temp files inside the workspace instead of locked system temp."""
    repo_root = Path(__file__).resolve().parents[1]
    temp_root = repo_root / ".pytest_tmp"
    session_temp = temp_root / f"run-{os.getpid()}-{uuid.uuid4().hex}"

    temp_root.mkdir(exist_ok=True)

    if config.option.basetemp is None:
        config.option.basetemp = str(session_temp)
