"""Load R nowcast parquet artifacts into the unified merge schema."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import List, Optional

import pandas as pd

from core.normalization_rules import add_tnved_columns, normalize_tnved_code
from core.schema import EXPECTED_SCHEMA

logger = logging.getLogger(__name__)

NOWCAST_REQUIRED_COLUMNS = frozenset(
    {"STRANA", "PERIOD", "TNVED", "NAPR", "TYPE", "STOIM", "NETTO"}
)

# EXPECTED_SCHEMA column order plus TYPE — single source of truth is core.schema.
NOWCAST_UNIFIED_COLUMNS = tuple(EXPECTED_SCHEMA) + ("TYPE",)


def transform_nowcast_to_unified(
    df: pd.DataFrame, start_year: Optional[int] = None
) -> pd.DataFrame:
    """Transform nowcast parquet to unified schema and keep only TYPE='pred' rows."""
    missing_cols = NOWCAST_REQUIRED_COLUMNS - set(df.columns)
    if missing_cols:
        logger.warning(
            "Nowcast file is missing required columns: %s. Skipping nowcast.",
            missing_cols,
        )
        return pd.DataFrame()

    nowcast_df = df.copy()
    nowcast_df["TYPE"] = nowcast_df["TYPE"].astype(str).str.strip().str.lower()
    nowcast_df = nowcast_df[nowcast_df["TYPE"] == "pred"].copy()

    if nowcast_df.empty:
        logger.info("Nowcast file has no rows with TYPE='pred'.")
        return pd.DataFrame()

    nowcast_df["PERIOD"] = pd.to_datetime(nowcast_df["PERIOD"], errors="coerce").dt.normalize()
    nowcast_df.dropna(subset=["PERIOD"], inplace=True)

    if start_year:
        initial_rows = len(nowcast_df)
        nowcast_df = nowcast_df[nowcast_df["PERIOD"].dt.year >= start_year].copy()
        if len(nowcast_df) < initial_rows:
            logger.info(
                "Filtered nowcast by start_year >= %s. Kept %s of %s rows.",
                start_year,
                len(nowcast_df),
                initial_rows,
            )

    if nowcast_df.empty:
        logger.info("Nowcast is empty after filtering.")
        return pd.DataFrame()

    nowcast_df = add_tnved_columns(nowcast_df)
    nowcast_df["STRANA"] = nowcast_df["STRANA"].astype(str).str.upper()
    nowcast_df["NAPR"] = nowcast_df["NAPR"].astype(str).str.strip()

    nowcast_df["EDIZM"] = None
    nowcast_df["EDIZM_ISO"] = None
    nowcast_df["KOL"] = None
    nowcast_df["STOIM"] = pd.to_numeric(nowcast_df["STOIM"], errors="coerce")
    nowcast_df["NETTO"] = pd.to_numeric(nowcast_df["NETTO"], errors="coerce")

    for col in NOWCAST_UNIFIED_COLUMNS:
        if col not in nowcast_df.columns:
            nowcast_df[col] = None

    return nowcast_df[list(NOWCAST_UNIFIED_COLUMNS)]


def _tnved_key_nowcast_overlap(value: object) -> str:
    """Canonical TNVED for (fact vs pred) cell join."""
    if pd.isna(value):
        return ""
    cleaned = re.sub(r"\.0$", "", str(value).strip()).strip()
    if not cleaned or cleaned.lower() == "nan":
        return ""
    return normalize_tnved_code(cleaned)


def drop_nowcast_rows_superseded_by_facts(
    merged_df: pd.DataFrame, logger_instance: logging.Logger
) -> pd.DataFrame:
    """Remove TYPE=pred rows when the same trade cell already has factual data."""
    if merged_df.empty or "TYPE" not in merged_df.columns:
        return merged_df

    type_norm = merged_df["TYPE"].astype(str).str.strip().str.lower()
    pred_mask = type_norm.eq("pred")
    if not pred_mask.any():
        return merged_df

    kp = pd.to_datetime(merged_df["PERIOD"], errors="coerce").dt.normalize()
    ks = merged_df["STRANA"].astype(str).str.strip().str.upper()
    kt = merged_df["TNVED"].map(_tnved_key_nowcast_overlap)
    kn = merged_df["NAPR"].astype(str).str.strip()

    fact_mask = (~pred_mask) & kp.notna()
    if not fact_mask.any():
        logger_instance.info(
            "No factual rows with valid PERIOD for nowcast supersession check; keeping all preds."
        )
        return merged_df

    fact_keys = pd.DataFrame(
        {"_kp": kp[fact_mask], "_ks": ks[fact_mask], "_kt": kt[fact_mask], "_kn": kn[fact_mask]}
    ).drop_duplicates()

    pred_keys = pd.DataFrame(
        {
            "_row": merged_df.index[pred_mask],
            "_kp": kp[pred_mask].values,
            "_ks": ks[pred_mask].values,
            "_kt": kt[pred_mask].values,
            "_kn": kn[pred_mask].values,
        }
    )
    overlap = pred_keys.merge(fact_keys, on=["_kp", "_ks", "_kt", "_kn"], how="inner")
    drop_n = len(overlap)
    if drop_n == 0:
        logger_instance.info(
            "Nowcast supersession: 0 pred rows removed (no overlap with factual keys)."
        )
        return merged_df

    logger_instance.info(
        "Dropped %s nowcast rows that duplicate factual "
        "(PERIOD, STRANA, TNVED, NAPR) cells.",
        f"{drop_n:,}",
    )
    return merged_df.drop(index=overlap["_row"])


def append_nowcast_data(
    all_dataframes: List[pd.DataFrame],
    *,
    include_nowcast: bool,
    nowcast_path: Path,
    excluded_countries_upper: List[str],
    start_year: Optional[int] = None,
) -> None:
    """Optionally append nowcast pred rows from R-produced parquet."""
    if not include_nowcast:
        logger.info(
            "Nowcast disabled (--no-nowcast); not loading data_processed/nowcast/nowcast.parquet."
        )
        return

    if not nowcast_path.exists():
        logger.info("Nowcast file not found, skipping: %s", nowcast_path)
        return

    logger.info("Loading nowcast data from %s", nowcast_path)
    nowcast_raw_df = pd.read_parquet(nowcast_path)
    nowcast_df = transform_nowcast_to_unified(nowcast_raw_df, start_year=start_year)
    if nowcast_df.empty:
        return

    if excluded_countries_upper:
        before_excl = len(nowcast_df)
        nowcast_df = nowcast_df[~nowcast_df["STRANA"].isin(excluded_countries_upper)].copy()
        excluded_count = before_excl - len(nowcast_df)
        if excluded_count > 0:
            logger.info(
                "Excluded %s nowcast rows by --exclude-countries filter.",
                f"{excluded_count:,}",
            )

    if not nowcast_df.empty:
        nowcast_df["SOURCE"] = "nowcast"
        all_dataframes.append(nowcast_df)
        logger.info("Loaded nowcast rows (TYPE='pred'): %s", f"{len(nowcast_df):,}")
