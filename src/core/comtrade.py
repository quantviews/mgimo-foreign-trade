"""Comtrade data loading and transformation to the unified schema."""

import logging
from pathlib import Path
from typing import List

import duckdb
import pandas as pd

from core.edizm import load_edizm_mapping
from core.normalization_rules import add_tnved_columns
from core.reference_tables import load_partner_mapping
from core.schema import EXPECTED_SCHEMA, PHYSICAL_COLUMNS
from core.physical_units import select_comtrade_quantity

logger = logging.getLogger(__name__)


def load_and_transform_comtrade(
    comtrade_db_path: Path,
    project_root: Path,
    exclude_countries: List[str],
    start_year: int = None
) -> pd.DataFrame:
    """
    Loads and transforms Comtrade data, excluding specified countries.

    Args:
        comtrade_db_path: Path to the Comtrade DuckDB database.
        project_root: Path to the project root for metadata loading.
        exclude_countries: List of ISO2 country codes to exclude.

    Returns:
        A DataFrame with Comtrade data transformed to the unified schema.
    """
    logger.info(f"Loading Comtrade data, excluding: {exclude_countries}")

    partner_mapping = load_partner_mapping(project_root)
    if not partner_mapping:
        return pd.DataFrame()

    edizm_mapping = load_edizm_mapping(project_root)
    if not edizm_mapping:
        logger.error("Could not load Edizm mapping, aborting Comtrade load.")
        return pd.DataFrame()

    # Convert ISO2 country codes to Comtrade M49 codes for the query
    # Create case-insensitive mapping: uppercase ISO2 -> M49
    country_to_m49 = {v.upper(): k for k, v in partner_mapping.items() if v}

    # Convert exclude list to uppercase and map to M49
    exclude_countries_upper = [c.upper() for c in exclude_countries]
    exclude_m49_codes = []
    for c in exclude_countries_upper:
        if c in country_to_m49:
            m49_code = country_to_m49[c]
            exclude_m49_codes.append(m49_code)
            logger.info(f"Excluding country '{c}' (M49 code: {m49_code}) from Comtrade data")
        else:
            logger.warning(f"Could not find M49 code for country: {c}")

    logger.info(f"Total countries to exclude from Comtrade: {len(exclude_m49_codes)}")

    try:
        conn = duckdb.connect(str(comtrade_db_path), read_only=True)

        # Diagnostic: List tables and schema in the database
        tables = conn.execute("SHOW TABLES;").fetchall()
        logger.info(f"Tables found in {comtrade_db_path}: {tables}")
        try:
            table_info = conn.execute("DESCRIBE comtrade_data;").df()
            logger.info(f"Schema for comtrade_data:\n{table_info}")
        except Exception as e:
            logger.warning(f"Could not describe comtrade_data table: {e}")

        # Build query with safe formatting (M49 codes are always integers, so safe to format)
        # Try to include qtyCode if it exists in the table
        try:
            # Check if qtyCode column exists
            test_query = "SELECT qtyCode FROM comtrade_data LIMIT 1"
            conn.execute(test_query)
            has_qty_code = True
            logger.info("Found qtyCode column in comtrade_data table")
        except:
            has_qty_code = False
            logger.info("qtyCode column not found in comtrade_data table, using qtyUnitCode/altQtyUnitCode")

        query_parts = ["SELECT"]
        query_parts.append("    period AS PERIOD,")
        query_parts.append("    reporterCode AS STRANA_CODE,")
        query_parts.append("    cmdCode AS TNVED,")
        query_parts.append("    CASE flowCode WHEN 'M' THEN 'ЭК' WHEN 'X' THEN 'ИМ' WHEN 'ЭК' THEN 'ЭК' WHEN 'ИМ' THEN 'ИМ' END AS NAPR,")
        query_parts.append("    qtyUnitCode,")
        query_parts.append("    altQtyUnitCode,")
        if has_qty_code:
            query_parts.append("    qtyCode,")
        query_parts.append("    primaryValue AS STOIM,")
        query_parts.append("    netWgt AS NETTO,")
        query_parts.append("    qty,")
        query_parts.append("    altQty,")
        available = set(table_info['column_name'])
        for raw, target in [('isQtyEstimated', 'SOURCE_QTY_ESTIMATED'), ('isNetWgtEstimated', 'SOURCE_NETTO_ESTIMATED')]:
            expression = raw if raw in available else 'NULL::BOOLEAN'
            query_parts.append(f"    {expression} AS {target},")
        query_parts[-1] = query_parts[-1].rstrip(',')
        query_parts.append("FROM comtrade_data")

        where_clauses = []

        if exclude_m49_codes:
            # M49 codes are always integers, safe to format directly
            # Validate all codes are integers for safety
            if not all(isinstance(code, int) for code in exclude_m49_codes):
                raise ValueError("All exclude_m49_codes must be integers")
            codes_str = ', '.join(map(str, exclude_m49_codes))
            where_clauses.append(f"reporterCode NOT IN ({codes_str})")

        if start_year:
            logger.info(f"Applying start_year filter to Comtrade data: year >= {start_year}")
            if not isinstance(start_year, int):
                raise ValueError("start_year must be an integer")
            where_clauses.append(f"refYear >= {start_year}")

        if where_clauses:
            query_parts.append("WHERE " + " AND ".join(where_clauses))

        query = "\n".join(query_parts)
        logger.info(f"Executing Comtrade query...")
        comtrade_df = conn.execute(query).fetchdf()
    except Exception as e:
        logger.error(f"Failed to query Comtrade data: {e}")
        return pd.DataFrame()
    finally:
        if 'conn' in locals():
            conn.close()

    if comtrade_df.empty:
        logger.info("Query returned no Comtrade data for the specified countries.")
        return pd.DataFrame()

    logger.info(f"Query returned {len(comtrade_df)} rows from Comtrade DB.")

    # Post-processing transformations
    logger.info("Transforming Comtrade data...")

    comtrade_df = select_comtrade_quantity(comtrade_df, edizm_mapping)

    comtrade_df['STRANA'] = comtrade_df['STRANA_CODE'].map(partner_mapping)

    # Ensure STRANA is uppercase for consistency
    comtrade_df['STRANA'] = comtrade_df['STRANA'].str.upper()

    # Convert EDIZM_CODE to int for proper mapping (edizm_mapping uses int keys)
    # Handle NaN values - convert to int only for non-null values
    # First convert to float, then to Int64 to handle NaN properly
    comtrade_df['EDIZM_CODE_int'] = pd.to_numeric(comtrade_df['EDIZM_CODE'], errors='coerce').astype('Int64')

    # Map EDIZM_CODE (int) to EDIZM (string abbreviation like "Bq")
    comtrade_df['EDIZM'] = comtrade_df['EDIZM_CODE_int'].map(edizm_mapping)
    comtrade_df.fillna({'EDIZM': 'N/A'}, inplace=True)

    # Diagnostic: Check if code 37 was mapped to "Bq"
    code_37_mask = comtrade_df['EDIZM_CODE_int'] == 37
    if code_37_mask.any():
        code_37_count = code_37_mask.sum()
        code_37_edizm = comtrade_df.loc[code_37_mask, 'EDIZM'].unique()
        logger.info(f"  - Found {code_37_count} rows with EDIZM_CODE = 37 (Becquerels)")
        logger.info(f"  - EDIZM_CODE 37 mapped to EDIZM values: {code_37_edizm}")
        if 'Bq' in code_37_edizm or 'BQ' in code_37_edizm:
            bq_count = (comtrade_df.loc[code_37_mask, 'EDIZM'].isin(['Bq', 'BQ', 'bq'])).sum()
            logger.info(f"  - Of these, {bq_count} rows have EDIZM = 'Bq'")

    # Drop temporary column
    comtrade_df.drop(columns=['EDIZM_CODE_int'], inplace=True)

    null_strana_count = comtrade_df['STRANA'].isnull().sum()
    if null_strana_count > 0:
        logger.warning(f"Found {null_strana_count} rows with reporter codes that could not be mapped to ISO2 codes. These will be dropped.")
        unmapped_codes = comtrade_df[comtrade_df['STRANA'].isnull()]['STRANA_CODE'].unique()
        logger.warning(f"Unmapped reporter codes (sample): {unmapped_codes[:10]}")

    comtrade_df.dropna(subset=['STRANA'], inplace=True)
    logger.info(f"{len(comtrade_df)} rows remaining after dropping unmapped countries.")

    # Verify unique countries in Comtrade data
    comtrade_countries = comtrade_df['STRANA'].unique()
    logger.info(f"Countries in Comtrade data after transformation: {sorted(comtrade_countries)}")

    if comtrade_df.empty:
        logger.warning("No Comtrade data remaining after transformation.")
        return pd.DataFrame()

    comtrade_df['EDIZM_ISO'] = None

    comtrade_df = add_tnved_columns(comtrade_df)

    # Ensure data types match the expected schema
    for col, expected_type in EXPECTED_SCHEMA.items():
        if col in comtrade_df.columns and str(comtrade_df[col].dtype) != expected_type:
            try:
                if 'datetime' in expected_type:
                    if col == 'PERIOD':
                        # Convert to datetime and normalize to remove time component
                        comtrade_df[col] = pd.to_datetime(comtrade_df[col], errors='coerce').dt.normalize()
                    else:
                        comtrade_df[col] = pd.to_datetime(comtrade_df[col])
                else:
                    comtrade_df[col] = comtrade_df[col].astype(expected_type)
            except (ValueError, TypeError) as e:
                logger.warning(f"Could not convert Comtrade column '{col}' to {expected_type}: {e}")

    # Reorder columns to match the main schema
    final_cols = [col for col in list(EXPECTED_SCHEMA) + PHYSICAL_COLUMNS if col in comtrade_df.columns]
    return comtrade_df[final_cols]


__all__ = ["load_and_transform_comtrade"]
