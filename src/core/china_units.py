"""Interpret China's primary/supplementary quantities before assigning units."""
import pandas as pd
from core.edizm import resolve_edizm_record


def load_year_units(directory):
    mapping = {}
    if directory is None:
        return mapping
    import csv, io
    for path in sorted(directory.glob('*-china.csv')):
        try:
            text = path.read_text(encoding='utf-8-sig')
        except UnicodeDecodeError:
            text = path.read_text(encoding='cp1252')
        year = int(path.name[:4])
        reader = csv.reader(io.StringIO(text))
        next(reader)
        for row in reader:
            if not row: continue
            if row[-1] == '': row = row[:-1]
            # Some descriptions contain unescaped quotes/commas. The five
            # trailing year/unit fields remain stable; never skip those codes.
            if len(row)<7 or row[-5] != str(year):
                raise ValueError(f'Invalid unit reference row in {path.name}: {row[0]}')
            mapping[year, row[0].zfill(8)] = (row[-3], row[-1])
    return mapping


def normalize_quantities(frame, mapping, common):
    out = frame.copy()
    def unit(value):
        value = str(value).strip()
        return '' if value.lower() in ('', '?', 'nan', 'none', '#n/a') else value
    def numeric(value):
        return pd.to_numeric(str(value).replace(',', '').strip(), errors='coerce')
    def record(value):
        if not value: return None
        aliases = {'Kilolitre': '130', 'Kilowatt': '214', 'Kilowatt-hour': '246', 'Million Bq': '323'}
        return resolve_edizm_record(aliases.get(value, value), common) or {'NAME': value, 'KOD': None}
    values = []
    for row in frame.to_dict('records'):
        year = int(str(row['PERIOD'])[:4])
        code = str(row['TNVED']).zfill(8)
        first, second = mapping.get((year, code), ('', ''))
        # Old collector saved primary Quantity under NETTO and dropped Unit.
        # Recover only from that year's codebook, never from another year's code.
        u1 = unit(row.get('Unit', '')) or unit(first)
        u2 = unit(row.get('Supplementary Unit', row.get('EDIZM', ''))) or unit(second)
        q1 = numeric(row.get('Quantity', row.get('NETTO')))
        q2 = numeric(row.get('Supplementary Quantity', row.get('KOL')))
        r1, r2 = record(u1), record(u2)
        kg1 = r1 and str(r1.get('KOD')) == '166'
        kg2 = r2 and str(r2.get('KOD')) == '166'
        mass_factor = {'166': 1, '163': .001, '168': 1000, '162': .0002}
        f1 = mass_factor.get(str(r1.get('KOD'))) if r1 else None
        f2 = mass_factor.get(str(r2.get('KOD'))) if r2 else None
        weight = q1 if kg1 else q2 if kg2 else q1*f1 if f1 else q2*f2 if f2 else float('nan')
        # A primary count must not be lost when the second unit is kg.
        quantity, rec = (q1, r1) if r1 and not kg1 else (q2, r2) if r2 and not kg2 else (float('nan'), None)
        selected_unit = u1 if r1 and not kg1 else u2
        quantity *= {'Kilowatt-hour': .001, 'Million Bq': 1e6}.get(selected_unit, 1)
        values.append((weight, quantity, rec.get('NAME') if rec else None, rec.get('KOD') if rec else None))
    out[['NETTO','KOL','EDIZM','EDIZM_ISO']] = pd.DataFrame(values, index=frame.index)
    return out
