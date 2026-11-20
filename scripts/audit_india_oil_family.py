"""Reconcile observed oil-family values with independent national HS4 reports.

Read-only. Agreement is at the published monetary precision; it never asserts
that an unreported HS8 code is zero. Cumulative checks can corroborate an existing
month only when it is the sole unresolved month and the cumulative sum is exact.
"""
import argparse
import hashlib
import json
from pathlib import Path
from decimal import Decimal

import duckdb
import pandas as pd
from bs4 import BeautifulSoup


def number(value):
    try:
        return float(Decimal(value.replace(',', '').strip()) * 1_000_000)
    except Exception:
        return None


def reconcile(local, reports):
    """Return per-month evidence; never change local values or infer native zeros."""
    output = []
    for row in local:
        period = row['period']
        choices = [r for r in reports if r['period'] == period]
        choices.sort(key=lambda r: r['requested_year'], reverse=True)
        selected = choices[0] if choices else None
        record = {**row, 'status': 'no_independent_report', 'evidence': None,
                  'difference_usd': None, 'rounding_bound_usd': (row['positive_rows'] + 1) * 5000}
        if selected:
            record.update(evidence=selected['file'], source_usd=selected['monthly_usd'])
            if selected['all_zero_response']:
                record['status'] = 'invalid_all_zero_source_response'
            elif selected['monthly_usd'] is not None and row['local_usd'] is not None:
                diff = row['local_usd'] - selected['monthly_usd']
                record['difference_usd'] = diff
                record['status'] = ('verified_monthly' if abs(diff) <= record['rounding_bound_usd'] + .01
                                    else 'unresolved_value_disagreement')
        output.append(record)
    # Do not derive values. Corroborate one existing value using an exact YTD check.
    for report in sorted(reports, key=lambda r: r['period']):
        if report['all_zero_response'] or report['ytd_usd'] is None:
            continue
        year, month = map(int, report['period'][:7].split('-'))
        window = [r for r in output if f'{year}-01-01' <= r['period'] <= report['period']]
        if len(window) != month or any(r['local_usd'] is None for r in window):
            continue
        unresolved = [r for r in window if not r['status'].startswith('verified_')]
        if len(unresolved) != 1:
            continue
        if abs(sum(r['local_usd'] for r in window) - report['ytd_usd']) < .01:
            target = unresolved[0]
            target.update(status='verified_exact_cumulative', cumulative_evidence=report['file'],
                          cumulative_period=report['period'], cumulative_total_usd=report['ytd_usd'])
    return output


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', default='db/unified_trade_data.duckdb')
    parser.add_argument('--cache', default='tmp/india-oil-check-20261119')
    parser.add_argument('--out', required=True)
    parser.add_argument('--fetch-early', action='store_true')
    args = parser.parse_args()
    out = Path(args.out); out.mkdir(parents=True, exist_ok=True)
    if args.fetch_early:
        from audit_india_oil_source import fetch
        for month in range(1, 13):
            fetch(out, 2020, month, 4, 'value')
    reports = []
    evidence = []
    for path in sorted(list(Path(args.cache).glob('*-hs4-value.html')) + list(out.glob('*-hs4-value.html'))):
        year, month = map(int, path.name[:7].split('-'))
        table = BeautifulSoup(path.read_bytes(), 'html.parser').find('table', id='example1')
        headers = [x.get_text(' ', strip=True) for x in table.find_all('th')]
        rows = [[x.get_text(' ', strip=True) for x in tr.find_all('td')]
                for tr in table.find_all('tr') if tr.find_all('td')]
        oil = [r for r in rows if len(r) > 7 and r[1] == '2709']
        if len(oil) != 1:
            continue
        evidence.append(dict(path=path.as_posix(), sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                             headers=headers, oil_row=oil[0]))
        for y, index, ytd in [(year, 4, 7), (year-1, 3, 6)]:
            assert str(y) in headers[index] and str(y) in headers[ytd]
            amounts = [number(r[index]) for r in rows if len(r) > index and r[1].isdigit()]
            reports.append(dict(period=f'{y}-{month:02}-01', requested_year=year,
                                monthly_usd=number(oil[0][index]), ytd_usd=number(oil[0][ytd]),
                                all_zero_response=bool(amounts) and all(v == 0 for v in amounts),
                                file=path.as_posix()))
    con = duckdb.connect(args.db, read_only=True)
    local = con.execute("""SELECT cast(PERIOD as varchar) period,sum(STOIM) local_usd,
        count(*) row_count,count(*) FILTER(WHERE STOIM>0) positive_rows,
        sum(NETTO) available_mass_kg,
        count(*) FILTER(WHERE STOIM>0 AND (NETTO IS NULL OR NETTO<=0)) unknown_positive_mass_rows,
        string_agg(TNVED,',' ORDER BY TNVED) codes
      FROM unified_trade_data WHERE STRANA='IN' AND NAPR='ЭК' AND TYPE='fact'
       AND SOURCE='national' AND TNVED4='2709' GROUP BY PERIOD ORDER BY PERIOD""").df().to_dict('records')
    reviewed = reconcile(local, reports)
    pd.DataFrame(reviewed).to_csv(out/'oil-monthly-reconciliation.csv', index=False)
    pd.DataFrame(reports).to_csv(out/'independent-reports.csv', index=False)
    (out/'source-evidence.json').write_text(json.dumps(evidence, ensure_ascii=False, indent=2), encoding='utf-8')
    summary = dict(months=len(reviewed), verified=sum(r['status'].startswith('verified_') for r in reviewed),
                   latest_window=[r for r in reviewed if r['period'] >= '2025-01-01'],
                   unresolved=[r for r in reviewed if not r['status'].startswith('verified_')],
                   database_modified=False, zero_imputation=False,
                   interpretation='Observed HS4 family reconciliation at source precision, not proof of zero at missing HS8 codes')
    (out/'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({k:v for k,v in summary.items() if k not in ('latest_window','unresolved')}, ensure_ascii=False))
    print('Unresolved:', [(r['period'], r['status']) for r in summary['unresolved']])


if __name__ == '__main__':
    main()
