"""Schema validation helpers for unified trade data."""

import logging
from pathlib import Path

import pandas as pd

from core.normalization_rules import add_tnved_columns

logger = logging.getLogger(__name__)

# Define the expected schema from data_model.md
EXPECTED_SCHEMA = {
    'NAPR': 'object',          # VARCHAR - торговый поток (ИМ/ЭК)
    'PERIOD': 'datetime64[ns]', # DATE - отчетный период (normalized to 00:00:00, DuckDB will save as DATE)
    'STRANA': 'object',         # VARCHAR - страна-отчет (ISO код)
    'TNVED': 'object',          # VARCHAR - код ТН ВЭД (8-10 знаков)
    'EDIZM': 'object',          # VARCHAR - единица измерения
    'EDIZM_ISO': 'object',      # VARCHAR - ISO код единицы измерения (опционально)
    'STOIM': 'float64',         # DECIMAL - стоимость в USD (в единицах, не в тысячах)
    'NETTO': 'float64',         # DECIMAL - вес нетто в кг
    'KOL': 'float64',           # DECIMAL - количество в дополнительной единице
    'TNVED4': 'object',         # VARCHAR - первые 4 знака TNVED
    'TNVED6': 'object',         # VARCHAR - первые 6 знаков TNVED
    'TNVED8': 'object',         # VARCHAR - первые 8 знаков TNVED
    'TNVED2': 'object'          # VARCHAR - первые 2 знака TNVED
}

def validate_schema(df: pd.DataFrame, filename: str) -> bool:
    """
    Validate DataFrame against expected schema.

    Args:
        df: DataFrame to validate
        filename: Name of the file for error reporting

    Returns:
        True if schema is valid, False otherwise


    """
    logger.info(f"Validating schema for {filename}")

    # Check if all required columns are present
    missing_cols = set(EXPECTED_SCHEMA.keys()) - set(df.columns)
    if missing_cols:
        logger.error(f"Missing columns in {filename}: {missing_cols}")
        return False

    # Check for extra columns
    extra_cols = set(df.columns) - set(EXPECTED_SCHEMA.keys())
    if extra_cols:
        logger.warning(f"Extra columns in {filename}: {extra_cols}")

    # Check data types (only for non-null values)
    for col, expected_type in EXPECTED_SCHEMA.items():
        if col in df.columns:
            actual_type = df[col].dtype

            # Convert period to date if it's not already
            if col == 'PERIOD':
                try:
                    # Convert to datetime and normalize to remove time component
                    # We'll use datetime64[ns] but normalized (time set to 00:00:00)
                    # DuckDB will recognize it as DATE when saving
                    if 'datetime' not in str(actual_type):
                        df[col] = pd.to_datetime(df[col]).dt.normalize()
                    elif actual_type != 'datetime64[ns]':
                        df[col] = pd.to_datetime(df[col]).dt.normalize()
                    else:
                        # Already datetime, just normalize to remove time
                        df[col] = df[col].dt.normalize()
                    actual_type = df[col].dtype
                except Exception as e:
                    logger.error(f"Failed to convert PERIOD to date in {filename}: {e}")
                    return False

            if actual_type != expected_type:
                logger.error(f"Column {col} has wrong type in {filename}: expected {expected_type}, got {actual_type}")
                return False

    # Validate specific values
    if 'NAPR' in df.columns:
        invalid_napr = df[~df['NAPR'].isin(['ИМ', 'ЭК'])]['NAPR'].unique()
        if len(invalid_napr) > 0:
            logger.error(f"Invalid NAPR values in {filename}: {invalid_napr}")
            return False

    if 'PERIOD' in df.columns:
        invalid_periods = df[df['PERIOD'].isnull()]
        if len(invalid_periods) > 0:
            logger.error(f"Null periods found in {filename}")
            return False

    logger.info(f"Schema validation passed for {filename}")
    return True


def smoke_check_merged_dataset(df: pd.DataFrame, label: str = "merged dataset") -> bool:
    """
    Smoke-checks the final merged DataFrame before it is written to DuckDB.

    Checks (all failures are logged as ERROR):
      1. Non-empty result after merge.
      2. All required columns from EXPECTED_SCHEMA are present.
      3. PERIOD is datetime64 and has no null values.
      4. NAPR contains only valid values: 'ИМ' or 'ЭК'.

    Returns True if every check passes, False otherwise.
    Callers should abort saving when False is returned.
    """
    passed = True

    # 1. Non-empty
    if df.empty:
        logger.error(f"SMOKE CHECK FAILED [{label}]: result is empty (0 rows after merge)")
        return False

    # 2. Required columns
    required = frozenset(EXPECTED_SCHEMA.keys())
    missing = required - set(df.columns)
    if missing:
        logger.error(
            f"SMOKE CHECK FAILED [{label}]: missing required columns: {sorted(missing)}"
        )
        passed = False

    # 3. PERIOD type and nullability
    if 'PERIOD' in df.columns:
        if not pd.api.types.is_datetime64_any_dtype(df['PERIOD']):
            logger.error(
                f"SMOKE CHECK FAILED [{label}]: PERIOD is not datetime64 "
                f"(got {df['PERIOD'].dtype})"
            )
            passed = False
        elif df['PERIOD'].isna().any():
            null_count = int(df['PERIOD'].isna().sum())
            logger.error(
                f"SMOKE CHECK FAILED [{label}]: PERIOD has {null_count:,} null values"
            )
            passed = False

    # 4. NAPR values
    if 'NAPR' in df.columns:
        invalid_napr = set(df['NAPR'].dropna().unique()) - {'ИМ', 'ЭК'}
        if invalid_napr:
            bad_rows = int(df['NAPR'].isin(invalid_napr).sum())
            logger.error(
                f"SMOKE CHECK FAILED [{label}]: invalid NAPR values in {bad_rows:,} rows: "
                f"{sorted(invalid_napr)}"
            )
            passed = False

    if passed:
        period_min = df['PERIOD'].min() if 'PERIOD' in df.columns else '?'
        period_max = df['PERIOD'].max() if 'PERIOD' in df.columns else '?'
        napr_vals = sorted(df['NAPR'].dropna().unique()) if 'NAPR' in df.columns else []
        logger.info(
            f"SMOKE CHECK PASSED [{label}]: {len(df):,} rows | "
            f"PERIOD [{period_min} – {period_max}] | "
            f"NAPR {napr_vals}"
        )

    return passed


def load_and_validate_file(file_path: Path, start_year: int = None) -> pd.DataFrame:
    """
    Load parquet file and validate schema.

    Args:
        file_path: Path to parquet file
        start_year: Optional year to filter data from

    Returns:
        Validated DataFrame or None if validation fails
    """
    try:
        logger.info(f"Loading {file_path}")
        df = pd.read_parquet(file_path)

        if start_year:
            if 'PERIOD' not in df.columns:
                logger.warning(f"Cannot filter by year: {file_path.name} has no PERIOD column.")
            else:
                # Convert to datetime and normalize to remove time component
                if 'datetime' not in str(df['PERIOD'].dtype):
                    df['PERIOD'] = pd.to_datetime(df['PERIOD'], errors='coerce').dt.normalize()
                else:
                    df['PERIOD'] = df['PERIOD'].dt.normalize()

                initial_rows = len(df)
                df = df[df['PERIOD'].dt.year >= start_year].copy()
                if len(df) < initial_rows:
                    logger.info(f"Filtered {file_path.name} by start_year >= {start_year}. Kept {len(df)} of {initial_rows} rows.")

        # Skip schema validation for fizob files (they have a different schema)
        if not file_path.name.startswith('fizob_'):
            df = add_tnved_columns(df)
            if not validate_schema(df, file_path.name):
                logger.error(f"Schema validation failed for {file_path.name}")
                return None
        else:
            logger.info(f"Skipping schema validation for {file_path.name} (fizob file with different schema)")

        logger.info(f"Successfully loaded {file_path.name}: {len(df)} rows")
        return df

    except Exception as e:
        logger.error(f"Failed to load {file_path}: {e}")
        return None


__all__ = [
    "EXPECTED_SCHEMA",
    "load_and_validate_file",
    "smoke_check_merged_dataset",
    "validate_schema",
]
