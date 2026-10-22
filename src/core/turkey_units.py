"""Do not interpret Turkish missing quantity sentinels as observed zero mass."""
def normalize_turkey_missing(frame):
    out = frame.copy()
    # Positive trade value with zero reported weight is unknown, not zero trade.
    missing = out['STOIM'].gt(0) & out['NETTO'].eq(0)
    out.loc[missing, 'NETTO'] = float('nan')
    unknown_unit = out['EDIZM'].isna() | out['EDIZM'].isin(['?', '-'])
    out.loc[unknown_unit & out['KOL'].eq(0), 'KOL'] = float('nan')
    return out
