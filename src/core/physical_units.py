"""Physical values and provenance: never pair a value with another field's unit."""
import pandas as pd


def select_comtrade_quantity(df, units):
    df = df.copy()
    primary = pd.to_numeric(df['qtyUnitCode'], errors='coerce')
    alternative = pd.to_numeric(df['altQtyUnitCode'], errors='coerce')
    qty = pd.to_numeric(df['qty'], errors='coerce')
    alt = pd.to_numeric(df['altQty'], errors='coerce')
    valid = set(units) - {-1, 8}
    # Prefer a positive supplementary quantity. A zero alternative must not
    # displace a positive primary quantity with the same unit.
    weight_codes = {8: 1, 15: 0.001, 21: 1000}
    nonweight = valid - set(weight_codes)
    primary_supplement = primary.isin(nonweight) & qty.gt(0)
    use_alt = alternative.isin(valid) & alt.gt(0) & ~(alternative.isin(weight_codes) & primary_supplement)
    use_primary = ~use_alt & primary.isin(valid) & qty.gt(0)
    df['EDIZM_CODE'] = alternative.where(use_alt, primary.where(use_primary))
    df['KOL'] = alt.where(use_alt, qty.where(use_primary))
    df['SOURCE_UNIT'] = df['EDIZM_CODE'].map(units)
    df['KOL_ESTIMATED'] = pd.Series(pd.NA, index=df.index, dtype='boolean')
    df.loc[use_primary, 'KOL_ESTIMATED'] = df.loc[use_primary, 'SOURCE_QTY_ESTIMATED'].astype('boolean')
    # No alternative estimate flag in this source schema: unknown is not false.
    df['NETTO_ESTIMATED'] = df['SOURCE_NETTO_ESTIMATED'].astype('boolean')
    df['PHYSICAL_QUALITY'] = ''
    zero_mass = df['STOIM'].gt(0) & df['NETTO'].eq(0)
    df.loc[zero_mass, 'NETTO'] = float('nan')
    df.loc[zero_mass, 'PHYSICAL_QUALITY'] = 'Нулевая масса при положительной стоимости: масса неизвестна'
    # Restore only explicit mass units, not carrying capacity/drained weight.
    primary_mass = df['NETTO'].isna() & primary.isin(weight_codes) & qty.gt(0)
    alt_mass = df['NETTO'].isna() & ~primary_mass & alternative.isin(weight_codes) & alt.gt(0)
    df.loc[primary_mass, 'NETTO'] = (qty * primary.map(weight_codes))[primary_mass]
    df.loc[primary_mass, 'NETTO_ESTIMATED'] = df.loc[primary_mass, 'SOURCE_QTY_ESTIMATED'].astype('boolean')
    df.loc[alt_mass, 'NETTO'] = (alt * alternative.map(weight_codes))[alt_mass]
    df.loc[alt_mass, 'NETTO_ESTIMATED'] = pd.NA
    recovered = primary_mass | alt_mass
    df.loc[recovered, 'PHYSICAL_QUALITY'] = 'Масса восстановлена из весовой единицы источника (кг, г или т)'
    conflict = alternative.eq(primary) & alternative.isin(valid) & alt.eq(0) & qty.gt(0)
    df.loc[conflict, 'PHYSICAL_QUALITY'] += '; Нулевое альтернативное количество: использовано положительное основное'
    return df


def ensure_physical_provenance(df):
    """Stable nullable schema for all sources; do not imply observed if unknown."""
    for col in ('SOURCE_QTY_ESTIMATED', 'SOURCE_NETTO_ESTIMATED', 'KOL_ESTIMATED', 'NETTO_ESTIMATED'):
        if col not in df:
            df[col] = pd.NA
        df[col] = df[col].astype('boolean')
    for col in ('SOURCE_UNIT', 'PHYSICAL_QUALITY'):
        if col not in df:
            df[col] = None
        df[col] = df[col].astype('string')
    return df
