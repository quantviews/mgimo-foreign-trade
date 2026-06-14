"""Centralized normalization rules for unified trade data."""

from __future__ import annotations

import logging
import re
from typing import Dict, Optional

import pandas as pd


TNVED_LENGTH = 10
TNVED_DERIVED_LEVELS = (2, 4, 6, 8)

KG_ISO_CODE = "166"
TONNE_ISO_CODE = "168"
BECQUEREL_NAME = "БЕККЕРЕЛЬ"
BQ_ALIAS = "BQ"

COUNTRY_UNIT_ALIAS_RECORDS = {
    # India / generic abbreviations
    "KGS": {"KOD": KG_ISO_CODE, "NAME": "КИЛОГРАММ"},
    "KG": {"KOD": KG_ISO_CODE, "NAME": "КИЛОГРАММ"},
    "KILOGRAM": {"KOD": KG_ISO_CODE, "NAME": "КИЛОГРАММ"},
    "T": {"KOD": TONNE_ISO_CODE, "NAME": "ТОННА, МЕТРИЧЕСКАЯ ТОННА (1000 КГ)"},
    "TON": {"KOD": TONNE_ISO_CODE, "NAME": "ТОННА, МЕТРИЧЕСКАЯ ТОННА (1000 КГ)"},
    "NOS": {"KOD": "796", "NAME": "ШТУКА"},
    "NO": {"KOD": "796", "NAME": "ШТУКА"},
    "U": {"KOD": "796", "NAME": "ШТУКА"},
    "PAIRS": {"KOD": "715", "NAME": "ПАРА"},
    "LTR": {"KOD": "112", "NAME": "ЛИТР"},
    "L": {"KOD": "112", "NAME": "ЛИТР"},
    "M2": {"KOD": "055", "NAME": "КВАДРАТНЫЙ МЕТР"},
    "M3": {"KOD": "113", "NAME": "КУБИЧЕСКИЙ МЕТР"},
    "M": {"KOD": "006", "NAME": "МЕТР"},
    "?": {"KOD": None, "NAME": "?"},
    "-": {"KOD": None, "NAME": "?"},

    # China unit descriptions
    "NUMBER OF ITEM": {"KOD": "796", "NAME": "ШТУКА"},
    "NUMBER OF ITEMS": {"KOD": "796", "NAME": "ШТУКА"},
    "PIECE": {"KOD": "796", "NAME": "ШТУКА"},
    "PAIR": {"KOD": "715", "NAME": "ПАРА"},
    "SQUARE METRE": {"KOD": "055", "NAME": "КВАДРАТНЫЙ МЕТР"},
    "METRE": {"KOD": "006", "NAME": "МЕТР"},
    "LITRE": {"KOD": "112", "NAME": "ЛИТР"},
    "CUBIC METRE": {"KOD": "113", "NAME": "КУБИЧЕСКИЙ МЕТР"},
    "CARAT": {"KOD": "162", "NAME": "МЕТРИЧЕСКИЙ КАРАТ(1КАРАТ=2*10(-4)КГ"},
    "GRAM": {"KOD": "163", "NAME": "ГРАММ"},
    "IN HUNDREDS": {"KOD": "797", "NAME": "СТО ШТУК"},
    "IN THOUSANDS": {"KOD": "798", "NAME": "ТЫСЯЧА ШТУК"},

    # Turkey raw units
    "KG/ÇİFT": {"KOD": "715", "NAME": "ПАРА"},
    "KG/METR E": {"KOD": "006", "NAME": "МЕТР"},
    "KG/1000A DET": {"KOD": "798", "NAME": "ТЫСЯЧА ШТУК"},
    "KG/KG P2O5": {"KOD": "865", "NAME": "КИЛОГРАММ ПЯТИОКИСИ ФОСФОРА"},
    "KG/ADET": {"KOD": "796", "NAME": "ШТУКА"},
    "KG/M3": {"KOD": "113", "NAME": "КУБИЧЕСКИЙ МЕТР"},
    "KG/KG K2O": {"KOD": "852", "NAME": "КИЛОГРАММ ОКСИДА КАЛИЯ"},
    "KG/KG MET.AM.": {"KOD": None, "NAME": "КИЛОГРАММ МЕТИЛАМИНА"},
    "KG/1000LI TRE": {"KOD": "130", "NAME": "1000 ЛИТРОВ"},
    "KG/CE-EL": {"KOD": "745", "NAME": "ЭЛЕМЕНТ"},
    "KG/LİTRE": {"KOD": "112", "NAME": "ЛИТР"},
    "KG/BAŞ": {"KOD": "836", "NAME": "ГОЛОВА"},
    "KG/KARA T": {"KOD": "162", "NAME": "МЕТРИЧЕСКИЙ КАРАТ(1КАРАТ=2*10(-4)КГ"},
    "KG/100AD ET": {"KOD": "797", "NAME": "СТО ШТУК"},
    "KG/KG N": {"KOD": "861", "NAME": "КИЛОГРАММ АЗОТА"},
    "KG/M2": {"KOD": "055", "NAME": "КВАДРАТНЫЙ МЕТР"},
    "KG/LT- ALK%100": {"KOD": "831", "NAME": "ЛИТР ЧИСТОГО (100%) СПИРТА"},
    "KG/KG H2O2": {"KOD": "841", "NAME": "КИЛОГРАММ ПЕРОКСИДА ВОДОРОДА"},
    "KG/GRAM": {"KOD": "163", "NAME": "ГРАММ"},
    "KG/KG U": {"KOD": "867", "NAME": "КИЛОГРАММ УРАНА"},
    "KG/1000M 3": {"KOD": "114", "NAME": "1000 КУБИЧЕСКИХ МЕТРОВ"},
    "KG/GI F/S": {"KOD": None, "NAME": "gi F/S"},
    "KG/CT-L": {"KOD": None, "NAME": "CT-L"},
    "G.T/ADET": {"KOD": "796", "NAME": "ШТУКА"},
    "KG/KG NET EDA": {"KOD": None, "NAME": "KG NET EDA"},
    "KG/KG %90 SDT": {"KOD": "845", "NAME": "КИЛОГРАММ СУХОГО НА 90 % ВЕЩЕСТВА"},
    "KG/KG KOH": {"KOD": "859", "NAME": "КИЛОГРАММ ГИДРОКСИДА КАЛИЯ"},
    "KG/KG NAOH": {"KOD": "863", "NAME": "КИЛОГРАММ ГИДРОКСИДА НАТРИЯ"},
}


def normalize_tnved_code(code: object, length: int = TNVED_LENGTH) -> str:
    """Normalize TNVED by preserving leading zeros and right-padding/truncating."""
    code_str = str(code).strip()
    if len(code_str) >= length:
        return code_str[:length]
    return code_str + "0" * (length - len(code_str))


def add_tnved_columns(df: pd.DataFrame, source_col: str = "TNVED") -> pd.DataFrame:
    """Normalize TNVED and generate TNVED2, TNVED4, TNVED6, TNVED8 columns."""
    df_processed = df.copy()
    if source_col not in df_processed.columns:
        return df_processed

    df_processed[source_col] = df_processed[source_col].apply(normalize_tnved_code)
    for level in TNVED_DERIVED_LEVELS:
        df_processed[f"TNVED{level}"] = df_processed[source_col].str[:level]

    return df_processed


def normalize_edizm_value(value: object) -> str:
    """Normalize raw EDIZM values before lookup in the common EDIZM map."""
    value_str = str(value).upper().strip()
    value_str = value_str.replace("³", "3").replace("²", "2")
    value_str = value_str.replace("KG/NET", "KG NET")
    value_str = re.sub(r"\s*\(\s*", " (", value_str)
    value_str = re.sub(r"\s*\)\s*", ")", value_str)
    value_str = re.sub(r"\s+", " ", value_str)
    return value_str.strip()


def resolve_edizm_record(
    value: object,
    common_edizm_map: Optional[Dict[str, Dict[str, str]]] = None,
) -> Optional[Dict[str, str]]:
    """Resolve a raw country-processor unit value to a common EDIZM record."""
    key = normalize_edizm_value(value)
    if common_edizm_map:
        record = common_edizm_map.get(key)
        if isinstance(record, dict):
            return record
    return COUNTRY_UNIT_ALIAS_RECORDS.get(key)


def resolve_edizm_records(
    values: pd.Series,
    common_edizm_map: Optional[Dict[str, Dict[str, str]]] = None,
) -> pd.Series:
    """Vector-friendly wrapper around resolve_edizm_record."""
    return values.apply(lambda value: resolve_edizm_record(value, common_edizm_map))


def standardize_edizm_columns(
    df: pd.DataFrame,
    common_edizm_map: Dict[str, Dict[str, str]],
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """Map raw EDIZM values to canonical EDIZM and EDIZM_ISO columns."""
    df_processed = df.copy()
    if "EDIZM" not in df_processed.columns:
        if logger:
            logger.warning("Cannot standardize EDIZM values: EDIZM column not found.")
        return df_processed

    df_processed["EDIZM_upper"] = df_processed["EDIZM"].apply(normalize_edizm_value)
    mapped_values = df_processed["EDIZM_upper"].map(common_edizm_map)

    df_processed["EDIZM"] = mapped_values.map(
        lambda value: value.get("NAME") if isinstance(value, dict) else None
    )
    df_processed["EDIZM_ISO"] = mapped_values.map(
        lambda value: value.get("KOD") if isinstance(value, dict) else None
    )

    if logger:
        unmapped_mask = df_processed["EDIZM"].isnull()
        if unmapped_mask.sum() > 0:
            logger.warning(
                f"{unmapped_mask.sum()} EDIZM values could not be mapped to a common standard."
            )
            unmapped_sample = df_processed.loc[unmapped_mask, "EDIZM_upper"].unique()
            logger.warning(f"Unmapped EDIZM sample: {unmapped_sample[:10]}")

        bq_mask = df_processed["EDIZM_upper"] == BQ_ALIAS
        if bq_mask.any():
            bq_count = bq_mask.sum()
            bq_mapped = df_processed.loc[bq_mask, "EDIZM"].notna().sum()
            logger.info(f"  - Found {bq_count} rows with EDIZM_upper = '{BQ_ALIAS}'")
            logger.info(f"  - Of these, {bq_mapped} were successfully mapped to canonical name")
            if bq_mapped < bq_count:
                bq_unmapped = df_processed.loc[
                    bq_mask & df_processed["EDIZM"].isna(), "EDIZM_upper"
                ].unique()
                logger.warning(f"  - Unmapped '{BQ_ALIAS}' values (sample): {bq_unmapped[:5]}")
                logger.info(
                    f"  - Checking if '{BECQUEREL_NAME}' exists in mapping: "
                    f"{BECQUEREL_NAME in common_edizm_map}"
                )
                logger.info(
                    f"  - Checking if '{BQ_ALIAS}' exists in mapping: {BQ_ALIAS in common_edizm_map}"
                )
                if BQ_ALIAS in common_edizm_map:
                    logger.info(f"  - '{BQ_ALIAS}' maps to: {common_edizm_map[BQ_ALIAS]}")
            else:
                bq_mapped_values = df_processed.loc[bq_mask, "EDIZM"].unique()
                logger.info(f"  - All '{BQ_ALIAS}' values mapped to: {bq_mapped_values}")

    df_processed.drop(columns=["EDIZM_upper"], inplace=True)
    return df_processed


def get_special_edizm_aliases(canonical_records: Dict[str, Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    """Return EDIZM aliases that are part of project-level special handling."""
    return {
        BQ_ALIAS: canonical_records.get(BECQUEREL_NAME),
        "BECQUEREL": canonical_records.get(BECQUEREL_NAME),
        "MILLION BQ": canonical_records.get("МИЛЛИОН БЕККЕРЕЛЕЙ"),
    }


def apply_special_edizm_cases(
    df: pd.DataFrame,
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """Apply project-specific KG, tonne and becquerel handling rules."""
    df_processed = df.copy()

    if "EDIZM" in df_processed.columns:
        becquerel_mask = df_processed["EDIZM"] == BECQUEREL_NAME
        num_becquerel_rows = becquerel_mask.sum()
        if logger:
            logger.info(f"Checking for {BECQUEREL_NAME} units to nullify KOL values...")
        if num_becquerel_rows > 0:
            if logger:
                logger.info(
                    f"Found {num_becquerel_rows:,} rows where EDIZM is {BECQUEREL_NAME}. "
                    "Setting KOL to NULL for these rows (values are too large)."
                )
            df_processed.loc[becquerel_mask, "KOL"] = None
    elif logger:
        logger.warning(f"Cannot perform {BECQUEREL_NAME} check: EDIZM column not found.")

    if "EDIZM_ISO" not in df_processed.columns:
        if logger:
            logger.warning("Cannot perform KG/Tonne checks: EDIZM_ISO column not found.")
        return df_processed

    if logger:
        logger.info("Checking for supplementary units in KG to avoid duplication with NETTO...")
    kg_rows_mask = df_processed["EDIZM_ISO"] == KG_ISO_CODE
    num_kg_rows = kg_rows_mask.sum()
    if num_kg_rows > 0:
        if logger:
            logger.info(
                f"Found {num_kg_rows:,} rows where the supplementary unit is KG. "
                "Setting KOL, EDIZM, and EDIZM_ISO to NULL for these rows."
            )
        df_processed.loc[kg_rows_mask, "KOL"] = None
        df_processed.loc[kg_rows_mask, "EDIZM"] = None
        df_processed.loc[kg_rows_mask, "EDIZM_ISO"] = None

    if logger:
        logger.info("Checking for supplementary units in Tonnes to convert or remove...")
    tonne_mask = (df_processed["EDIZM_ISO"] == TONNE_ISO_CODE) & df_processed["KOL"].notna()
    num_tonne_rows = tonne_mask.sum()
    if num_tonne_rows <= 0:
        return df_processed

    if logger:
        logger.info(f"Found {num_tonne_rows:,} rows with supplementary unit in Tonnes.")

    netto_missing_mask = tonne_mask & (
        (df_processed["NETTO"].isnull()) | (df_processed["NETTO"] == 0)
    )
    num_to_convert = netto_missing_mask.sum()
    if num_to_convert > 0:
        if logger:
            logger.info(f"  - Converting {num_to_convert:,} Tonne values to KG and filling NETTO.")
        df_processed.loc[netto_missing_mask, "NETTO"] = (
            df_processed.loc[netto_missing_mask, "KOL"] * 1000
        )
        df_processed.loc[netto_missing_mask, "KOL"] = None
        df_processed.loc[netto_missing_mask, "EDIZM"] = None
        df_processed.loc[netto_missing_mask, "EDIZM_ISO"] = None

    tonne_mask = (df_processed["EDIZM_ISO"] == TONNE_ISO_CODE) & df_processed["KOL"].notna()
    netto_present_mask = tonne_mask & df_processed["NETTO"].notna() & (df_processed["NETTO"] != 0)
    num_to_remove = netto_present_mask.sum()
    if num_to_remove > 0:
        if logger:
            logger.info(
                f"  - Removing {num_to_remove:,} redundant Tonne values as NETTO is already populated."
            )
        df_processed.loc[netto_present_mask, "KOL"] = None
        df_processed.loc[netto_present_mask, "EDIZM"] = None
        df_processed.loc[netto_present_mask, "EDIZM_ISO"] = None

    return df_processed
