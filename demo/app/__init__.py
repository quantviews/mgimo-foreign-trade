"""Demo chat app package.

Makes trade_tools_shared importable: it ships as a sibling of this app/
directory in the Docker image (COPY shared/trade_tools_shared); outside
Docker it lives in ../../shared at the repo root, so add that for local/dev
runs and tests.
"""
import sys
from pathlib import Path

_SHARED_DIR = Path(__file__).resolve().parent.parent.parent / "shared"
if (_SHARED_DIR / "trade_tools_shared").is_dir() and str(_SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(_SHARED_DIR))
