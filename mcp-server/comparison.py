"""Compatibility import for local calculation tests; production uses the API."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "api"))
from app.comparison import compare, compare_fizob, month, shift, number
