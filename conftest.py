"""Repo-wide pytest bootstrap.

Makes `shared/trade_tools_shared` importable for every test suite in this
repo (demo/tests, mcp-server/tests, ...), regardless of which directory
pytest is invoked from. pytest loads conftest.py files from every ancestor
directory of a collected test before importing the test module itself, so
this runs before any test's own imports.
"""
import sys
from pathlib import Path

_SHARED_DIR = Path(__file__).resolve().parent / "shared"
if str(_SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(_SHARED_DIR))
