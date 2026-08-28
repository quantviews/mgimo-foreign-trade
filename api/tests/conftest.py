import sys
from pathlib import Path

# Make the `app` package importable (api/ on sys.path).
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
