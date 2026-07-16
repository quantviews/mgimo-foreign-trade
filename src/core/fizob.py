"""Fizob parquet transformation to the unified fizob_index schema."""

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def transform_fizob_to_unified(df: pd.DataFrame, file_stem: str) -> pd.DataFrame:
    """
    Transform a fizob parquet DataFrame to unified fizob_index schema.
    Schema: STRANA, NAPR, PERIOD, tn_level, tn_code, fizob, fizob_bp
    """
    df = df.copy()
    if 'PERIOD' in df.columns:
        df['PERIOD'] = pd.to_datetime(df['PERIOD'], errors='coerce').dt.normalize()

    if file_stem == 'fizob_total':
        # Total level: aggregated across all TNVED, tn_level=0, tn_code='0'
        if 'fizob' not in df.columns or 'fizob_bp' not in df.columns:
            logger.warning(f"fizob_total missing fizob/fizob_bp columns, skipping")
            return pd.DataFrame()
        out = df[['STRANA', 'NAPR', 'PERIOD']].copy()
        out['tn_level'] = 0
        out['tn_code'] = df['TNVED2'].fillna(0).astype(int).astype(str) if 'TNVED2' in df.columns else '0'
        out['fizob'] = df['fizob'].values
        out['fizob_bp'] = df['fizob_bp'].values
        return out

    # Level-specific: fizob_2, fizob_4, fizob_6
    mapping = {
        'fizob_2': (2, 'TNVED2', 'fizob2', 'fizob2_bp'),
        'fizob2': (2, 'TNVED2', 'fizob2', 'fizob2_bp'),
        'fizob_4': (4, 'TNVED4', 'fizob4', 'fizob4_bp'),
        'fizob4': (4, 'TNVED4', 'fizob4', 'fizob4_bp'),
        'fizob_6': (6, 'TNVED6', 'fizob6', 'fizob6_bp'),
        'fizob6': (6, 'TNVED6', 'fizob6', 'fizob6_bp'),
    }
    if file_stem not in mapping:
        logger.warning(f"Unknown fizob file stem '{file_stem}', skipping")
        return pd.DataFrame()

    level, tnved_col, fizob_col, fizob_bp_col = mapping[file_stem]
    for col in [tnved_col, fizob_col, fizob_bp_col]:
        if col not in df.columns:
            logger.warning(f"{file_stem} missing column {col}, skipping")
            return pd.DataFrame()

    out = df[['STRANA', 'NAPR', 'PERIOD', tnved_col]].copy()
    out = out.rename(columns={tnved_col: 'tn_code'})
    out['tn_level'] = level
    out['fizob'] = df[fizob_col].values
    out['fizob_bp'] = df[fizob_bp_col].values
    return out


__all__ = ["transform_fizob_to_unified"]
