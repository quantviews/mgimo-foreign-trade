"""Create an isolated, audited zero supplement. No working database writes."""
import argparse
import sys
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from core.publication_registry import confirmed_absence_rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('database')
    parser.add_argument('output')
    parser.add_argument('--registry', default=str(ROOT / 'metadata/publication_registry.json'))
    args = parser.parse_args()
    out = Path(args.output)
    if out.exists():
        raise FileExistsError(out)
    with duckdb.connect(args.database, read_only=True) as con:
        observations = con.execute('SELECT DISTINCT STRANA,NAPR,TNVED,PERIOD FROM unified_trade_data').fetchdf()
    zeros = confirmed_absence_rows(observations, args.registry, ROOT)
    zeros['PERIOD'] = pd.to_datetime(zeros.PERIOD)
    zeros.to_parquet(out, index=False)
    print(f'Confirmed absent observations: {len(zeros)}')


if __name__ == '__main__':
    main()
