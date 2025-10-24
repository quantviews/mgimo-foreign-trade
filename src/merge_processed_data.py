#!/usr/bin/env python3
"""
Script to merge processed parquet files (data_processed/) and save to DuckDB.

This script:
1. Reads processed parquet files from data_processed/ folder
2. Validates each dataset against the data model schema
3. Optionally, loads and transforms Comtrade data for missing countries
4. Merges all datasets into one unified dataset
5. Excludes countries specified in --exclude-countries argument
6. start_year argument to filter data from this year onwards
7. Saves the merged dataset to DuckDB format in db/unified_trade_data.duckdb
"""

import pandas as pd
import duckdb
from pathlib import Path
import logging
import argparse
import json
from typing import Dict, List

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define the expected schema from data_model.md
EXPECTED_SCHEMA = {
    'NAPR': 'object',          # VARCHAR - торговый поток (ИМ/ЭК)
    'PERIOD': 'datetime64[ns]', # DATE - отчетный период
    'STRANA': 'object',         # VARCHAR - страна-отчет (ISO код)
    'TNVED': 'object',          # VARCHAR - код ТН ВЭД (8-10 знаков)
    'EDIZM': 'object',          # VARCHAR - единица измерения
    'EDIZM_ISO': 'object',      # VARCHAR - ISO код единицы измерения (опционально)
    'STOIM': 'float64',         # DECIMAL - стоимость в ТЫСЯЧАХ USD
    'NETTO': 'float64',         # DECIMAL - вес нетто в кг
    'KOL': 'float64',           # DECIMAL - количество в дополнительной единице
    'TNVED4': 'object',         # VARCHAR - первые 4 знака TNVED
    'TNVED6': 'object',         # VARCHAR - первые 6 знаков TNVED
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
            
            # Convert period to datetime if it's not already
            if col == 'PERIOD' and actual_type != 'datetime64[ns]':
                try:
                    df[col] = pd.to_datetime(df[col])
                    actual_type = df[col].dtype
                except Exception as e:
                    logger.error(f"Failed to convert PERIOD to datetime in {filename}: {e}")
                    return False
            
            if actual_type != expected_type:
                logger.error(f"Column {col} has wrong type in {filename}: expected {expected_type}, got {actual_type}")
    
    # Validate specific values
    if 'NAPR' in df.columns:
        invalid_napr = df[~df['NAPR'].isin(['ИМ', 'ЭК'])]['NAPR'].unique()
        if len(invalid_napr) > 0:
            logger.error(f"Invalid NAPR values in {filename}: {invalid_napr}")
    
    if 'PERIOD' in df.columns:
        invalid_periods = df[df['PERIOD'].isnull()]
        if len(invalid_periods) > 0:
            logger.error(f"Null periods found in {filename}")
            return False
    
    logger.info(f"Schema validation passed for {filename}")
    return True

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
                if df['PERIOD'].dtype != 'datetime64[ns]':
                    df['PERIOD'] = pd.to_datetime(df['PERIOD'], errors='coerce')
                
                initial_rows = len(df)
                df = df[df['PERIOD'].dt.year >= start_year].copy()
                if len(df) < initial_rows:
                    logger.info(f"Filtered {file_path.name} by start_year >= {start_year}. Kept {len(df)} of {initial_rows} rows.")

        # Validate schema
        if not validate_schema(df, file_path.name):
            logger.error(f"Schema validation failed for {file_path.name}")
            return None
        
        logger.info(f"Successfully loaded {file_path.name}: {len(df)} rows")
        return df
        
    except Exception as e:
        logger.error(f"Failed to load {file_path}: {e}")
        return None

def generate_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate derived TNVED columns if they don't exist or validate them.
    
    Args:
        df: DataFrame to process
        
    Returns:
        DataFrame with validated/generated derived columns
    """


    df_processed = df.copy()
    
    # Ensure TNVED columns are strings to avoid .str accessor errors
    tnved_columns = ['TNVED', 'TNVED2', 'TNVED4', 'TNVED6']
    for col in tnved_columns:
        if col in df_processed.columns:
            df_processed[col] = df_processed[col].astype(str).str.zfill(len(col)-2 if col != 'TNVED' else 8)
    
    # Generate TNVED2, TNVED4, TNVED6 if they don't exist or validate them
    if 'TNVED' in df_processed.columns:
        if 'TNVED2' in df_processed.columns:
            # Validate TNVED2
            expected_tnved2 = df_processed['TNVED'].str[:2]
            invalid_tnved2 = df_processed[df_processed['TNVED2'] != expected_tnved2]
            if len(invalid_tnved2) > 0:
                logger.warning(f"Found {len(invalid_tnved2)} rows with invalid TNVED2, correcting...")
                df_processed['TNVED2'] = expected_tnved2
        else:
            df_processed['TNVED2'] = df_processed['TNVED'].str[:2]
        
        if 'TNVED4' in df_processed.columns:
            # Validate TNVED4
            expected_tnved4 = df_processed['TNVED'].str[:4]
            invalid_tnved4 = df_processed[df_processed['TNVED4'] != expected_tnved4]
            if len(invalid_tnved4) > 0:
                logger.warning(f"Found {len(invalid_tnved4)} rows with invalid TNVED4, correcting...")
                df_processed['TNVED4'] = expected_tnved4
        else:
            df_processed['TNVED4'] = df_processed['TNVED'].str[:4]
        
        if 'TNVED6' in df_processed.columns:
            # Validate TNVED6
            expected_tnved6 = df_processed['TNVED'].str[:6]
            invalid_tnved6 = df_processed[df_processed['TNVED6'] != expected_tnved6]
            if len(invalid_tnved6) > 0:
                logger.warning(f"Found {len(invalid_tnved6)} rows with invalid TNVED6, correcting...")
                df_processed['TNVED6'] = expected_tnved6
        else:
            df_processed['TNVED6'] = df_processed['TNVED'].str[:6]
    
    return df_processed

def merge_datasets(datasets: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Merge multiple datasets into one unified dataset.
    
    Args:
        datasets: Dictionary of country names to DataFrames
        
    Returns:
        Merged DataFrame
    """
    if not datasets:
        raise ValueError("No datasets to merge")
    
    logger.info("Merging datasets...")
    
    # Add SOURCE column to each dataset before merging
    dataframes_with_source = []
    for source_name, df in datasets.items():
        df_with_source = df.copy()
        df_with_source['SOURCE'] = 'national'
        dataframes_with_source.append(df_with_source)
    
    # Concatenate all dataframes
    merged_df = pd.concat(dataframes_with_source, ignore_index=True)
    
    # Sort by PERIOD and STRANA for consistent ordering
    merged_df = merged_df.sort_values(['PERIOD', 'STRANA', 'TNVED'])
    
    logger.info(f"Successfully merged {len(datasets)} datasets: {len(merged_df)} total rows")
    
    return merged_df

def save_to_duckdb(df: pd.DataFrame, output_path: Path, table_name: str = 'unified_trade_data'):
    """
    Save DataFrame to DuckDB database.
    
    Args:
        df: DataFrame to save
        output_path: Path to DuckDB file
        table_name: Name of the table in database
    """
    logger.info(f"Saving merged data to DuckDB: {output_path}")
    
    try:
        # Connect to DuckDB
        conn = duckdb.connect(str(output_path))
        
        # Register DataFrame
        conn.register('merged_data', df)
        
        # Create table from DataFrame
        conn.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS 
        SELECT * FROM merged_data
        """)
        
        # Get row count for verification
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        row_count = result[0]
        
        conn.close()
        
        logger.info(f"Successfully saved {row_count} rows to {output_path}")
        
    except Exception as e:
        logger.error(f"Failed to save to DuckDB: {e}")
        raise

def load_partner_mapping(project_root: Path) -> Dict[int, str]:
    """Loads Comtrade partner code (M49) to ISO2 mapping from JSON."""
    mapping_file = project_root / "metadata" / "comtrate-partnerAreas.json"
    if not mapping_file.exists():
        logger.error(f"Partner mapping file not found at {mapping_file}")
        return {}
    
    with open(mapping_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # M49 codes are numeric, ISO2 are strings
    mapping = {
        int(item['id']): item.get('PartnerCodeIsoAlpha2')
        for item in data.get('results', []) if item.get('PartnerCodeIsoAlpha2')
    }
    return mapping

def load_edizm_mapping(project_root: Path) -> Dict[int, str]:
    """Loads Comtrade qtyCode to qtyAbbr mapping from JSON."""
    mapping_file = project_root / "metadata" / "comtradte-QuantityUnits.json"
    if not mapping_file.exists():
        logger.error(f"Edizm mapping file not found at {mapping_file}")
        return {}
    
    with open(mapping_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    mapping = {
        item['qtyCode']: item.get('qtyAbbr')
        for item in data.get('results', [])
    }
    return mapping

def load_common_edizm_mapping(project_root: Path) -> Dict[str, Dict[str, str]]:
    """Loads common EDIZM mapping from edizm.csv."""
    mapping_file = project_root / "metadata" / "edizm.csv"
    if not mapping_file.exists():
        logger.error(f"Common EDIZM mapping file not found at {mapping_file}")
        return {}
    
    try:
        df = pd.read_csv(mapping_file, dtype={'KOD': str})
        
        # Create a flexible mapping from various names to a standard representation
        mapping = {}
        # Simple mappings from abbreviation to code
        simple_map = {
            "МЕТР": "м", "КВАДРАТНЫЙ МЕТР": "м2", 
            "КИЛОГРАММ": "кг",
            "ГРАММ": "г", 
            "КУБИЧЕСКИЙ МЕТР": "м3", 
            "ЛИТР": "л",
            "ШТУКА": "шт", "ПАРА": "пары", "СТО ШТУК": "100 шт",
            "ТЫСЯЧА ШТУК": "1000 шт", "МЕТРИЧЕСКИЙ КАРАТ(1КАРАТ=2*10(-4)КГ": "карат"
        }
        
        for _, row in df.iterrows():
            common_name = simple_map.get(row['NAME'], row['NAME'])
            record = {'KOD': row['KOD'], 'COMMON_NAME': common_name}
            
            # Map from KOD, NAME, SHORT_NAME, and common_name
            mapping[str(row['KOD'])] = record
            mapping[row['NAME']] = record
            if pd.notna(row['SHORT_NAME']):
                mapping[row['SHORT_NAME']] = record
            mapping[common_name] = record

        # Add aliases for comtrade values
        mapping.update({
            'kg': mapping['КИЛОГРАММ'], 'u': mapping['ШТУКА'],
            'l': mapping['ЛИТР'], 'm²': mapping['КВАДРАТНЫЙ МЕТР'],
            'm³': mapping['КУБИЧЕСКИЙ МЕТР'], 'm': mapping['МЕТР'],
            '2u': mapping['ПАРА'], 'carat': mapping['МЕТРИЧЕСКИЙ КАРАТ(1КАРАТ=2*10(-4)КГ'],
            '1000u': mapping['ТЫСЯЧА ШТУК']
        })
            
        return mapping
    except Exception as e:
        logger.error(f"Failed to load common EDIZM mapping: {e}")
        return {}

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
    country_to_m49 = {v: k for k, v in partner_mapping.items()}
    exclude_m49_codes = [
        country_to_m49[c] for c in exclude_countries if c in country_to_m49
    ]
    logger.info(f"Excluding M49 codes: {exclude_m49_codes}")

    try:
        conn = duckdb.connect(str(comtrade_db_path), read_only=True)
        
        # Diagnostic: List tables in the database
        tables = conn.execute("SHOW TABLES;").fetchall()
        logger.info(f"Tables found in {comtrade_db_path}: {tables}")
        
        where_clauses = []
        if exclude_m49_codes:
            codes_str = ', '.join(map(str, exclude_m49_codes))
            where_clauses.append(f"reporterCode NOT IN ({codes_str})")
        
        if start_year:
            logger.info(f"Applying start_year filter to Comtrade data: year >= {start_year}")
            where_clauses.append(f"refYear >= {start_year}")

        where_clause = ""
        if where_clauses:
            where_clause = "WHERE " + " AND ".join(where_clauses)
            
        query = f"""
            SELECT
                period AS PERIOD,
                reporterCode AS STRANA_CODE,
                cmdCode AS TNVED,
                CASE flowCode WHEN 'M' THEN 'ИМ' WHEN 'X' THEN 'ЭК' END AS NAPR,
                qtyUnitCode AS EDIZM_CODE,
                primaryValue AS STOIM,
                netWgt AS NETTO,
                qty AS KOL
            FROM comtrade_data
            {where_clause}
        """
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
    comtrade_df['STRANA'] = comtrade_df['STRANA_CODE'].map(partner_mapping)
    comtrade_df['EDIZM'] = comtrade_df['EDIZM_CODE'].map(edizm_mapping)
    comtrade_df.fillna({'EDIZM': 'N/A'}, inplace=True)
    
    null_strana_count = comtrade_df['STRANA'].isnull().sum()
    if null_strana_count > 0:
        logger.warning(f"Found {null_strana_count} rows with reporter codes that could not be mapped to ISO2 codes. These will be dropped.")
        unmapped_codes = comtrade_df[comtrade_df['STRANA'].isnull()]['STRANA_CODE'].unique()
        logger.warning(f"Unmapped reporter codes (sample): {unmapped_codes[:10]}")

    comtrade_df.dropna(subset=['STRANA'], inplace=True)
    logger.info(f"{len(comtrade_df)} rows remaining after dropping unmapped countries.")

    if comtrade_df.empty:
        logger.warning("No Comtrade data remaining after transformation.")
        return pd.DataFrame()
        
    comtrade_df['EDIZM_ISO'] = None

    # Generate derived TNVED columns
    comtrade_df['TNVED'] = comtrade_df['TNVED'].astype(str)
    comtrade_df['TNVED2'] = comtrade_df['TNVED'].str.slice(0, 2)
    comtrade_df['TNVED4'] = comtrade_df['TNVED'].str.slice(0, 4)
    comtrade_df['TNVED6'] = comtrade_df['TNVED'].str.slice(0, 6)
    
    # Ensure data types match the expected schema
    for col, expected_type in EXPECTED_SCHEMA.items():
        if col in comtrade_df.columns and str(comtrade_df[col].dtype) != expected_type:
            try:
                if 'datetime' in expected_type:
                    comtrade_df[col] = pd.to_datetime(comtrade_df[col])
                else:
                    comtrade_df[col] = comtrade_df[col].astype(expected_type)
            except (ValueError, TypeError) as e:
                logger.warning(f"Could not convert Comtrade column '{col}' to {expected_type}: {e}")

    # Reorder columns to match the main schema
    final_cols = [col for col in EXPECTED_SCHEMA.keys() if col in comtrade_df.columns]
    return comtrade_df[final_cols]
    
def main():
    """Main function to orchestrate the merging process."""
    parser = argparse.ArgumentParser(
        description="Merge processed national data and optionally include Comtrade data."
    )
    parser.add_argument(
        '--include-comtrade',
        action='store_true',
        help="Include Comtrade data for countries not present in national data."
    )
    parser.add_argument(
        '--start-year',
        type=int,
        default=None,
        help="Filter all data to include records from this year onwards."
    )
    parser.add_argument(
        '--exclude-countries',
        type=str,
        nargs='+',
        default=[],
        help="List of ISO2 country codes to exclude from the merge (e.g., IN CN)."
    )
    args = parser.parse_args()

    # Define paths using the script's location for robustness
    project_root = Path(__file__).resolve().parent.parent
    data_processed_dir = project_root / "data_processed"
    db_dir = project_root / "db"
    output_db_path = db_dir / "unified_trade_data.duckdb"
    comtrade_db_path = db_dir / "comtrade.db"
    
    # Ensure output directory exists
    db_dir.mkdir(exist_ok=True)
    
    logger.info("Starting data merging process...")
    
    # Find all parquet files in data_processed
    parquet_files = list(data_processed_dir.glob("*.parquet"))
    logger.info(f"Found {len(parquet_files)} parquet files: {[f.name for f in parquet_files]}")
    
    excluded_countries_upper = [c.upper() for c in args.exclude_countries]

    # Load and validate national datasets
    national_datasets = {}
    if parquet_files:
        for file_path in parquet_files:
            country_code = file_path.stem.replace('_full', '').upper()
            if country_code in excluded_countries_upper:
                logger.info(f"Skipping {file_path.name} as per --exclude-countries argument.")
                continue

            df = load_and_validate_file(file_path, start_year=args.start_year)
            if df is not None:
                df_processed = generate_derived_columns(df)
                national_datasets[country_code.lower()] = df_processed
    else:
        logger.warning("No national parquet files found in data_processed directory.")

    all_dataframes = []
    national_countries_iso = []

    # Process national data
    for df in national_datasets.values():
        df['SOURCE'] = 'national'
        all_dataframes.append(df)
        if 'STRANA' in df.columns and not df.empty:
            national_countries_iso.append(df['STRANA'].iloc[0])
    
    # Process Comtrade data if flag is set
    if args.include_comtrade:
        if not comtrade_db_path.exists():
            logger.error(f"Comtrade database not found at {comtrade_db_path}. Cannot include Comtrade data.")
        else:
            # Always exclude national data countries from Comtrade pull
            # And also add any user-specified exclusions
            countries_to_exclude_from_comtrade = list(set(national_countries_iso + excluded_countries_upper))

            comtrade_df = load_and_transform_comtrade(
                comtrade_db_path, 
                project_root, 
                exclude_countries=countries_to_exclude_from_comtrade,
                start_year=args.start_year
            )
            if not comtrade_df.empty:
                comtrade_df['SOURCE'] = 'comtrade'
                all_dataframes.append(comtrade_df)

    if not all_dataframes:
        logger.error("No data available to merge.")
        return

    # Merge all datasets
    merged_df = pd.concat(all_dataframes, ignore_index=True)
    merged_df = merged_df.sort_values(['PERIOD', 'STRANA', 'TNVED'])

    # Standardize EDIZM column
    logger.info("Standardizing EDIZM column...")
    common_edizm_map = load_common_edizm_mapping(project_root)
    if common_edizm_map:
        # Normalize original EDIZM values before mapping
        merged_df['EDIZM_upper'] = merged_df['EDIZM'].str.upper().str.strip()
        
        # Map to common representation
        mapped_values = merged_df['EDIZM_upper'].map(common_edizm_map)
        
        # Update EDIZM and EDIZM_ISO
        merged_df['EDIZM'] = mapped_values.map(lambda x: x['COMMON_NAME'] if pd.notna(x) else None)
        merged_df['EDIZM_ISO'] = mapped_values.map(lambda x: x['KOD'] if pd.notna(x) else None)
        
        # Handle unmapped values
        unmapped_count = merged_df['EDIZM'].isnull().sum()
        if unmapped_count > 0:
            logger.warning(f"{unmapped_count} EDIZM values could not be mapped to a common standard.")
            unmapped_sample = merged_df[merged_df['EDIZM'].isnull()]['EDIZM_upper'].unique()
            logger.warning(f"Unmapped EDIZM sample: {unmapped_sample[:10]}")
            
        merged_df.drop(columns=['EDIZM_upper'], inplace=True)
    else:
        logger.error("Could not standardize EDIZM values due to mapping load failure.")

    # Display summary statistics
    logger.info("=== MERGE SUMMARY ===")
    logger.info(f"Total rows: {len(merged_df)}")
    logger.info(f"Unique countries: {merged_df['STRANA'].nunique()}")
    logger.info(f"Date range: {merged_df['PERIOD'].min()} to {merged_df['PERIOD'].max()}")
    
    logger.info("Rows by source:")
    source_counts = merged_df['SOURCE'].value_counts()
    for source, count in source_counts.items():
        logger.info(f"  {source}: {count:,} rows")
        
    logger.info("Rows by country:")
    country_counts = merged_df.groupby('SOURCE')['STRANA'].value_counts()
    logger.info(str(country_counts))
    
    # Show EDIZM counts by country
    logger.info("EDIZM counts by country:")
    edizm_counts = merged_df.groupby(['STRANA', 'EDIZM']).size().reset_index(name='count')
    edizm_counts = edizm_counts.sort_values(['STRANA', 'count'], ascending=[True, False])
    for strana, group in edizm_counts.groupby('STRANA'):
        logger.info(f"  Country: {strana}")
        for _, row in group.head(5).iterrows(): # Log top 5 EDIZM for each country
            logger.info(f"    - {row['EDIZM']}: {row['count']:,} rows")

    # Save to DuckDB
    save_to_duckdb(merged_df, output_db_path)

if __name__ == "__main__":
    main()