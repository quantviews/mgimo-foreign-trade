"""Download India tariff-line evidence, never overwrite source/serving data."""
import json
import sys
import time
import argparse
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from collectors.comtrade_collector import load_api_key


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--consignment',action='store_true')
    args=parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    out = root / 'tmp/india-comtrade-20261119'
    out.mkdir(parents=True, exist_ok=True)
    key = load_api_key(root)
    periods = [202403] + [y*100+m for y in (2025,2026) for m in range(1,13)
                          if y==2025 or m<=8]
    endpoint = 'https://comtradeapi.un.org/data/v1/getTariffline/C/M/HS'
    for period in periods:
        prefix='tariff-consignment' if args.consignment else 'tariff-allcodes'
        path = out / f'{prefix}-{period}.json'
        if path.exists():
            continue
        params = dict(reporterCode=699, partnerCode=643, period=period,
                      flowCode='M,X', maxRecords=100000, includeDesc='true')
        if args.consignment:
            params.pop('partnerCode')
            params.update(partner2Code=643,flowCode='M')
        for attempt in range(4):
            try:
                response = requests.get(endpoint, params={**params,'subscription-key':key}, timeout=120)
            except requests.RequestException as exc:
                raise RuntimeError(f'Comtrade network failure ({type(exc).__name__}) for {period}') from None
            if response.status_code!=429:
                break
            print('Rate limit; waiting before retry',period,flush=True)
            time.sleep(35)
        # Never print exception URLs containing credentials.
        if response.status_code!=200:
            raise RuntimeError(f'Comtrade HTTP {response.status_code} for {period}')
        data = response.json()
        rows = data.get('data')
        if not rows or len(rows)>=100000 or data.get('count',len(rows))!=len(rows):
            raise RuntimeError(f'Missing/truncated response: {period}')
        for row in rows:
            partner_field='partner2Code' if args.consignment else 'partnerCode'
            if row['reporterCode']!=699 or row[partner_field]!=643 or int(row['period'])!=period:
                raise RuntimeError('Unexpected response scope')
        path.write_text(json.dumps(dict(endpoint=endpoint,parameters=params,response=data),
                                   ensure_ascii=False),encoding='utf-8')
        print(period, len(rows), flush=True)
        time.sleep(3)


if __name__=='__main__':
    main()
