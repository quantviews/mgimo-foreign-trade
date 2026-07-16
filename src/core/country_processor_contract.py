"""Shared contract for country processor modules.

Country processors may keep country-specific extraction logic, but they should
all finish through this module so their outputs stay merge-compatible.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd


COUNTRY_OUTPUT_COLUMNS = (
    "NAPR",
    "PERIOD",
    "STRANA",
    "TNVED",
    "EDIZM",
    "EDIZM_ISO",
    "STOIM",
    "NETTO",
    "KOL",
    "TNVED4",
    "TNVED6",
    "TNVED2",
)

COUNTRY_NUMERIC_COLUMNS = ("STOIM", "NETTO", "KOL")
COUNTRY_TNVED_PREFIX_COLUMNS = {
    "TNVED2": 2,
    "TNVED4": 4,
    "TNVED6": 6,
}

NAPR_NORMALIZATION = {
    "1": "ИМ",
    "2": "ЭК",
    "IMPORT": "ИМ",
    "EXPORT": "ЭК",
    "M": "ЭК",
    "X": "ИМ",
    "ИМ": "ИМ",
    "ЭК": "ЭК",
}

# Partner-country flow -> RF perspective: what the partner exports, Russia imports.
NAPR_MIRROR = {
    "ИМ": "ЭК",
    "ЭК": "ИМ",
}


@dataclass(frozen=True)
class CountryProcessorInput:
    """Standard inputs accepted by country processors."""

    raw_data_dir: Path
    output_file: Path
    metadata_dir: Optional[Path] = None
    edizm_file: Optional[Path] = None
    country_code: Optional[str] = None

    @classmethod
    def from_paths(
        cls,
        raw_data_dir: Path,
        output_file: Path,
        *,
        country_code: Optional[str] = None,
        metadata_dir: Optional[Path] = None,
        edizm_file: Optional[Path] = None,
    ) -> "CountryProcessorInput":
        """Build a normalized input object from legacy path arguments."""
        raw_data_dir = Path(raw_data_dir)
        output_file = Path(output_file)
        if edizm_file is not None:
            edizm_file = Path(edizm_file)
        if metadata_dir is None and edizm_file is not None:
            metadata_dir = edizm_file.parent
        if metadata_dir is not None:
            metadata_dir = Path(metadata_dir)
        return cls(raw_data_dir, output_file, metadata_dir, edizm_file, country_code)


def normalize_napr_value(value: object) -> object:
    """Normalize trade-flow labels to project-standard ИМ/ЭК values."""
    if pd.isna(value):
        return value
    value_str = str(value).strip().upper()
    return NAPR_NORMALIZATION.get(value_str, value)


def mirror_napr_value(value: object) -> object:
    """Mirror a partner-country flow into the RF perspective (ИМ<->ЭК)."""
    normalized = normalize_napr_value(value)
    return NAPR_MIRROR.get(normalized, normalized)


def finalize_country_output(
    df: pd.DataFrame,
    *,
    country_code: Optional[str] = None,
    sort_by: Iterable[str] = ("PERIOD", "NAPR", "TNVED"),
    drop_duplicates: bool = True,
) -> pd.DataFrame:
    """Apply the shared post-processing contract to a country DataFrame."""
    out = df.copy()

    for col in COUNTRY_OUTPUT_COLUMNS:
        if col not in out.columns:
            out[col] = None

    out["NAPR"] = out["NAPR"].map(normalize_napr_value)
    out["PERIOD"] = pd.to_datetime(out["PERIOD"], errors="coerce").dt.normalize()

    if country_code is not None:
        out["STRANA"] = country_code
    out["STRANA"] = out["STRANA"].astype(str).str.upper()

    out["TNVED"] = out["TNVED"].astype(str).str.strip()
    for col, length in COUNTRY_TNVED_PREFIX_COLUMNS.items():
        out[col] = out["TNVED"].str[:length]

    for col in COUNTRY_NUMERIC_COLUMNS:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out = out[list(COUNTRY_OUTPUT_COLUMNS)]

    if drop_duplicates:
        out = out.drop_duplicates()

    existing_sort_cols = [col for col in sort_by if col in out.columns]
    if existing_sort_cols:
        out = out.sort_values(by=existing_sort_cols)

    return out.reset_index(drop=True)


def assert_country_output_contract(df: pd.DataFrame, *, expected_strana: Optional[str] = None) -> None:
    """Raise AssertionError if a country processor output violates the contract."""
    assert not df.empty, "Output DataFrame must not be empty"
    missing = set(COUNTRY_OUTPUT_COLUMNS) - set(df.columns)
    assert not missing, f"Missing required columns: {missing}"

    invalid_napr = set(df["NAPR"].dropna().unique()) - {"ИМ", "ЭК"}
    assert not invalid_napr, f"Invalid NAPR values: {invalid_napr}"

    assert pd.api.types.is_datetime64_any_dtype(df["PERIOD"]), (
        f"PERIOD must be datetime64, got {df['PERIOD'].dtype}"
    )

    for col in COUNTRY_NUMERIC_COLUMNS:
        assert pd.api.types.is_numeric_dtype(df[col]), (
            f"{col} must be numeric, got {df[col].dtype}"
        )

    assert df["TNVED"].dtype == object, "TNVED must be string (object dtype)"
    for col, length in COUNTRY_TNVED_PREFIX_COLUMNS.items():
        assert (df[col] == df["TNVED"].str[:length]).all(), (
            f"{col} is not consistent with the first {length} chars of TNVED"
        )

    if expected_strana is not None:
        assert (df["STRANA"] == expected_strana).all(), (
            f"Expected STRANA='{expected_strana}', got: {df['STRANA'].unique()}"
        )


def save_country_output(
    df: pd.DataFrame,
    output_file: Path,
    *,
    logger: Optional[logging.Logger] = None,
) -> None:
    """Persist a finalized country processor DataFrame to parquet."""
    output_file = Path(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    if logger:
        logger.info(f"Сохранение объединённого набора в {output_file}")
    try:
        df.to_parquet(output_file, index=False)
        if logger:
            logger.info(f"Успешно сохранено. Всего строк: {len(df)}")
    except ImportError:
        if logger:
            logger.error("Не установлен pyarrow. Установите: pip install pyarrow")
        raise
