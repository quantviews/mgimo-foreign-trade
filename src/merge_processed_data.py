#!/usr/bin/env python3
"""
Script to merge processed parquet files and save to DuckDB.

This script:
1. Reads processed parquet files from data_processed/ folder
2. Validates each dataset against the data model schema
3. Merges all datasets into one unified dataset
4. Saves the merged dataset to DuckDB format
"""

import pandas as pd
import duckdb
from pathlib import Path
import logging
from typing import Dict

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
    'STRANA': 'object',         # VARCHAR - страна-партнер (ISO код)
    'TNVED': 'object',          # VARCHAR - код ТН ВЭД (8-10 знаков)
    'EDIZM': 'object',          # VARCHAR - единица измерения
    'STOIM': 'float64',         # DECIMAL - стоимость в USD
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

def load_and_validate_file(file_path: Path) -> pd.DataFrame:
    """
    Load parquet file and validate schema.
    
    Args:
        file_path: Path to parquet file
        
    Returns:
        Validated DataFrame or None if validation fails
    """
    try:
        logger.info(f"Loading {file_path}")
        df = pd.read_parquet(file_path)
        
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

def main():
    """Main function to orchestrate the merging process."""
    # Define paths
    data_processed_dir = Path("../data_processed")
    db_dir = Path("../db")
    output_db_path = db_dir / "unified_trade_data.duckdb"
    
    # Ensure output directory exists
    db_dir.mkdir(exist_ok=True)
    
    logger.info("Starting data merging process...")
    
    # Find all parquet files in data_processed
    parquet_files = list(data_processed_dir.glob("*.parquet"))
    logger.info(f"Found {len(parquet_files)} parquet files: {[f.name for f in parquet_files]}")
    
    if not parquet_files:
        logger.error("No parquet files found in data_processed directory")
        return
    
    # Load and validate each file
    datasets = {}
    for file_path in parquet_files:
        country_name = file_path.stem.replace('_full', '')  # Extract country name
        df = load_and_validate_file(file_path)
        
        if df is not None:
            # Generate derived columns
            df_processed = generate_derived_columns(df)
            datasets[country_name] = df_processed
    
    if not datasets:
        logger.error("No valid datasets loaded")
        return
    
    # Merge all datasets
    merged_df = merge_datasets(datasets)
    
    # Display summary statistics
    logger.info("=== MERGE SUMMARY ===")
    logger.info(f"Total sources: {len(datasets)}")
    logger.info(f"Total rows: {len(merged_df)}")
    logger.info(f"Unique countries: {merged_df['STRANA'].nunique()}")
    logger.info(f"Date range: {merged_df['PERIOD'].min()} to {merged_df['PERIOD'].max()}")
    logger.info(f"Unique TNVED codes: {merged_df['TNVED'].nunique()}")
    
    # Show data by source
    logger.info("Data source:")
    logger.info(f"  national: {len(merged_df):,} rows")
    
    # Show data by country
    logger.info("Rows by country:")
    country_counts = merged_df['STRANA'].value_counts().sort_index()
    for country, count in country_counts.items():
        logger.info(f"  {country}: {count:,} rows")
    
    # Save to DuckDB
    save_to_duckdb(merged_df, output_db_path)

if __name__ == "__main__":
    main()