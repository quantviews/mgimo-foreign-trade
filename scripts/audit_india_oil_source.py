"""Read-only MEIDB oil reconciliation and ranked local Fisher coverage blockers.

Writes evidence to a new/local audit directory, never to source or serving files.
HS4 monetary totals are independently requested; physical units are checked at
HS8 because MEIDB documents quantity availability only at that level.
"""
import argparse
import hashlib
import json
import time
from pathlib import Path

import duckdb
import requests
from bs4 import BeautifulSoup

URL = 'https://tradestat.commerce.gov.in/meidb/country_wise_all_commodities_import'


def fetch(out, year, month, level, metric):
    name = f'{year}-{month:02}-hs{level}-{metric}'
    path = out / (name + '.html')
    params = dict(cwcimMonth=month, cwcimYear=year, cwcimallcount=344,
                  cwcimCommodityLevel=level, cwcimReportVal=1 if metric == 'value' else 2,
                  cwcimReportYear=2)
    if not path.exists():
        with requests.Session() as session:
            r = session.get(URL, timeout=45)
            r.raise_for_status()
            token = BeautifulSoup(r.content, 'html.parser').find('input', {'name': '_token'})
            if token is None:
                raise ValueError('No form token')
            r = session.post(URL, data={**params, '_token': token['value']}, timeout=90)
            r.raise_for_status()
            soup = BeautifulSoup(r.content, 'html.parser')
            if soup.find('table', id='example1') is None:
                raise ValueError(f'No result table: {name}')
            # Retain source content without disposable public form tokens.
            for el in soup.select('input[name="_token"],meta[name="csrf-token"]'):
                el.decompose()
            path.write_text(str(soup), encoding='utf-8')
        time.sleep(.4)
    body = path.read_bytes()
    table = BeautifulSoup(body, 'html.parser').find('table', id='example1')
    headers = [x.get_text(' ', strip=True) for x in table.find_all('th')]
    rows = [[x.get_text(' ', strip=True) for x in tr.find_all('td')]
            for tr in table.find_all('tr') if tr.find_all('td')]
    value_index = 4 if metric == 'value' else 5
    if str(year) not in headers[value_index] or str(year-1) not in headers[value_index-1]:
        raise ValueError(f'Unexpected period columns in {name}: {headers}')
    amounts = [number(r[value_index]) for r in rows
               if len(r)>value_index and r[1].isdigit()]
    evidence = dict(url=URL, parameters=params, sha256=hashlib.sha256(body).hexdigest(),
                    headers=headers,
                    current_report_all_zero=bool(amounts) and all(v == 0 for v in amounts),
                    oil_rows=[r for r in rows if len(r)>1 and r[1].startswith('2709')])
    (out / (name+'.json')).write_text(json.dumps(evidence, ensure_ascii=False, indent=2), encoding='utf-8')
    print(name, 'oil rows:',len(evidence['oil_rows']), flush=True)
    return evidence


def number(text):
    try:
        return float(text.replace(',', '').strip())
    except ValueError:
        return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--out', default='tmp/india-oil-check-20261119')
    parser.add_argument('--db', default='db/unified_trade_data.duckdb')
    args = parser.parse_args()
    out = Path(args.out); out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(args.db, read_only=True)
    comparisons = []
    # Each response supplies the same month of the preceding year too.
    periods = [(y,m) for y in (2022,2024,2026) for m in range(1,13) if y<2026 or m<=9]
    periods += [(2025,m) for m in (10,11,12)]
    for year,month in periods:
        evidence = fetch(out, year, month, 4, 'value')
        fetch(out, year, month, 4, 'quantity')
        assert len(evidence['oil_rows']) == 1
        row = evidence['oil_rows'][0]
        for y,index in ((year-1,3),(year,4)):
            period = f'{y}-{month:02}-01'
            source = number(row[index])
            local = con.execute("""SELECT sum(STOIM),count(*),count(*) FILTER(WHERE STOIM>0),
                sum(NETTO),count(*) FILTER(WHERE NETTO IS NULL AND STOIM>0)
                FROM unified_trade_data WHERE STRANA='IN' AND NAPR='ЭК'
                AND TYPE='fact' AND SOURCE='national' AND TNVED LIKE '2709%' AND PERIOD=?""",[period]).fetchone()
            # Source and original HS8 costs rounded to .01 million USD.
            tolerance = (local[2]+1)*5000
            delta = None if source is None or local[0] is None else source*1e6-local[0]
            comparisons.append(dict(period=period,request=f'{year}-{month:02}',source_usd=None if source is None else source*1e6,
                current_report_all_zero=evidence['current_report_all_zero'] if y==year else None,
                local_usd=local[0],difference_usd=delta,rounding_tolerance_usd=tolerance,
                within_rounding=None if delta is None else abs(delta)<=tolerance+.01,
                local_rows=local[1],local_known_mass_kg=local[3],positive_rows_missing_mass=local[4]))
    (out/'oil-value-comparison.json').write_text(json.dumps(comparisons,ensure_ascii=False,indent=2),encoding='utf-8')
    # Ask whether HS4 quantity is actually supplied; never infer kg from HS4.
    fetch(out,2026,9,4,'quantity')
    for y,m in [(2022,3),(2022,4),(2024,1),(2024,4),(2026,9)]:
        fetch(out,y,m,8,'quantity')
    # Expected grid matches existing Fisher: historical code keys, not invented zeros.
    blockers = con.execute("""WITH f AS (
      SELECT * FROM unified_trade_data WHERE STRANA='IN' AND TYPE='fact' AND SOURCE='national'
    ), per_code AS (
      SELECT NAPR,TNVED,min(PERIOD) first_row,max(PERIOD) last_row,
        count(distinct PERIOD) FILTER(WHERE PERIOD BETWEEN '2025-10-01' AND '2026-09-01') current_months,
        count(distinct PERIOD) FILTER(WHERE PERIOD BETWEEN '2025-01-01' AND '2025-12-01') base_months,
        sum(STOIM) FILTER(WHERE PERIOD BETWEEN '2025-10-01' AND '2026-09-01') current_usd,
        sum(STOIM) FILTER(WHERE PERIOD BETWEEN '2025-01-01' AND '2025-12-01') base_usd,
        sum(STOIM) FILTER(WHERE PERIOD BETWEEN '2025-10-01' AND '2026-09-01' AND NETTO>0) mass_covered_usd,
        sum(STOIM) FILTER(WHERE PERIOD BETWEEN '2025-10-01' AND '2026-09-01' AND KOL>0 AND EDIZM IS NOT NULL) quantity_covered_usd
      FROM f GROUP BY NAPR,TNVED
    ) SELECT *,current_usd/sum(current_usd) OVER(PARTITION BY NAPR) current_share,
      base_usd/sum(base_usd) OVER(PARTITION BY NAPR) base_share
      FROM per_code ORDER BY greatest(coalesce(current_usd,0),coalesce(base_usd,0)) DESC""").df()
    blockers.to_csv(out/'all-code-coverage.csv',index=False)
    gaps=blockers[(blockers.current_months<12)|(blockers.base_months<12)]
    gaps.to_csv(out/'ranked-coverage-blockers.csv',index=False)
    summary=[]
    for direction,group in blockers.groupby('NAPR'):
        complete=(group.current_months==12)&(group.base_months==12)
        summary.append(dict(direction=direction,codes=len(group),complete_both=int(complete.sum()),
            current_usd=float(group.current_usd.sum()),base_usd=float(group.base_usd.sum()),
            complete_current_value_share=float(group.loc[complete,'current_usd'].sum()/group.current_usd.sum()),
            complete_base_value_share=float(group.loc[complete,'base_usd'].sum()/group.base_usd.sum())))
    (out/'coverage-summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf-8')
    print('Saved audit:',out,flush=True)


if __name__ == '__main__':
    main()
