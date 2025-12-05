#!/usr/bin/env python3
"""
Script to extract data slices from unified database by country and 2-digit TNVED code.

This script:
1. Connects to unified_trade_data.duckdb
2. Uses unified_trade_data_enriched view to include country and TNVED names
3. Filters data by country (STRANA) and 2-digit TNVED code (TNVED2)
4. Saves each slice as CSV file to data_interim_csv/ folder
5. Uses polars for data processing
"""

import polars as pl
import duckdb
from pathlib import Path
import logging
import argparse
import json
from typing import Optional, Dict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_turkey_original_names(turkey_codes_dir: Path) -> Dict[str, str]:
    """
    Load original commodity names from Turkey JSON files.
    
    Args:
        turkey_codes_dir: Path to data_raw/turkey/hs_codes_json directory
        
    Returns:
        Dictionary mapping 8-digit HS code -> commodity name
    """
    logger.info(f"Loading Turkey original names from {turkey_codes_dir}")
    
    if not turkey_codes_dir.exists():
        logger.warning(f"Directory does not exist: {turkey_codes_dir}")
        return {}
    
    code_names = {}
    json_files = sorted(turkey_codes_dir.glob("turkey_codes*.json"))
    
    if not json_files:
        logger.warning(f"No JSON files found in {turkey_codes_dir}")
        return {}
    
    logger.info(f"Found {len(json_files)} JSON files to process")
    
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Turkey JSON files are dictionaries with HS8 codes as keys
            for hs8_code, commodity_name in data.items():
                original_code = str(hs8_code).strip()
                
                # Ensure code is 8 digits (pad with zeros on the left if needed)
                if len(original_code) < 8:
                    original_code = original_code.zfill(8)
                elif len(original_code) > 8:
                    original_code = original_code[:8]
                
                # Store commodity name (keep the most recent if code appears in multiple files)
                if commodity_name:
                    code_names[original_code] = str(commodity_name).strip()
            
            logger.info(f"  Processed {json_file.name}: {len(data)} records")
            
        except Exception as e:
            logger.error(f"Failed to process {json_file.name}: {e}")
            continue
    
    logger.info(f"Loaded {len(code_names)} Turkey original names")
    return code_names


def extract_slice(
    db_path: Path,
    output_dir: Path,
    country: Optional[str] = None,
    tnved2: Optional[str] = None,
    tnved4: Optional[str] = None,
    year: Optional[int] = None,
    project_root: Optional[Path] = None,
    output_format: str = 'csv'
) -> None:
    """
    Extract data slice from unified database.
    
    Args:
        db_path: Path to unified_trade_data.duckdb
        output_dir: Directory to save output files
        country: ISO2 country code to filter (if None, extracts for all countries)
        tnved2: 2-digit TNVED code to filter (if None, extracts for all codes)
        tnved4: 4-digit TNVED code to filter (if specified, overrides tnved2)
        year: Year to filter (if None, extracts for all years)
        project_root: Project root path (for loading Turkey names)
        output_format: Output format - 'csv' or 'excel' (default: 'csv')
    """
    if not db_path.exists():
        logger.error(f"Database not found at {db_path}")
        return
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Connecting to database: {db_path}")
    conn = duckdb.connect(str(db_path), read_only=True)
    
    try:
        # Build query using enriched view to include country and TNVED names
        query = "SELECT * FROM unified_trade_data_enriched WHERE 1=1"
        params = []
        
        if country:
            query += " AND STRANA = ?"
            params.append(country.upper())
        
        # If tnved4 is specified, use it (and derive tnved2 from it)
        if tnved4:
            tnved4_normalized = tnved4.zfill(4)
            query += " AND TNVED4 = ?"
            params.append(tnved4_normalized)
            # Also filter by TNVED2 derived from TNVED4
            tnved2_from_4 = tnved4_normalized[:2]
            query += " AND TNVED2 = ?"
            params.append(tnved2_from_4)
        elif tnved2:
            query += " AND TNVED2 = ?"
            params.append(tnved2.zfill(2))  # Ensure 2 digits with leading zeros
        
        if year:
            query += " AND EXTRACT(YEAR FROM PERIOD) >= ?"
            params.append(year)
        
        logger.info(f"Executing query: {query}")
        if params:
            logger.info(f"Parameters: {params}")
        
        # Execute query and convert to polars DataFrame
        df_pandas = conn.execute(query, params).fetchdf()
        
        if df_pandas.empty:
            logger.warning("Query returned no data")
            return
        
        # Convert to polars
        df = pl.from_pandas(df_pandas)
        logger.info(f"Retrieved {len(df):,} rows")
        
        # Add original Turkey names if country is TR
        if country and country.upper() == 'TR' and project_root:
            turkey_codes_dir = project_root / 'data_raw' / 'turkey' / 'hs_codes_json'
            turkey_names = load_turkey_original_names(turkey_codes_dir)
            
            if turkey_names:
                # Match 8-digit codes from Turkey JSON with TNVED codes
                # TNVED codes are 10 digits, we need first 8 digits
                def get_turkey_name(tnved_code: str) -> str:
                    if not tnved_code or len(tnved_code) < 8:
                        return ''
                    hs8_code = tnved_code[:8]
                    return turkey_names.get(hs8_code, '')
                
                # Add column with original Turkey names
                df = df.with_columns(
                    pl.col('TNVED').map_elements(get_turkey_name, return_dtype=pl.Utf8).alias('TNVED_NAME_ORIGINAL_TURKEY')
                )
                logger.info(f"Added original Turkey names column")
            else:
                # Add empty column if no names loaded
                df = df.with_columns(pl.lit('').alias('TNVED_NAME_ORIGINAL_TURKEY'))
                logger.warning("Could not load Turkey original names, added empty column")
        
        # Determine output filename and extension
        file_ext = '.xlsx' if output_format.lower() == 'excel' else '.csv'
        
        if country and (tnved4 or tnved2):
            # Single slice: country and code specified
            if tnved4:
                code_part = f"TNVED4_{tnved4.zfill(4)}"
            else:
                code_part = f"TNVED2_{tnved2.zfill(2)}"
            year_part = f"_from_{year}" if year else ""
            filename = f"{country.upper()}_{code_part}{year_part}{file_ext}"
            output_path = output_dir / filename
            
            # Save based on format
            if output_format.lower() == 'excel':
                df.write_excel(output_path)
            else:
                df.write_csv(output_path)
            logger.info(f"Saved to {output_path}")
        else:
            # Multiple slices: group by country and/or TNVED2
            if country:
                # Group by TNVED2 only
                for tnved2_val in sorted(df['TNVED2'].unique().to_list()):
                    df_slice = df.filter(pl.col('TNVED2') == tnved2_val)
                    filename = f"{country.upper()}_TNVED2_{tnved2_val}{file_ext}"
                    output_path = output_dir / filename
                    if output_format.lower() == 'excel':
                        df_slice.write_excel(output_path)
                    else:
                        df_slice.write_csv(output_path)
                    logger.info(f"Saved {len(df_slice):,} rows to {output_path}")
            elif tnved2:
                # Group by country only
                for country_val in sorted(df['STRANA'].unique().to_list()):
                    df_slice = df.filter(pl.col('STRANA') == country_val)
                    filename = f"{country_val}_TNVED2_{tnved2.zfill(2)}{file_ext}"
                    output_path = output_dir / filename
                    if output_format.lower() == 'excel':
                        df_slice.write_excel(output_path)
                    else:
                        df_slice.write_csv(output_path)
                    logger.info(f"Saved {len(df_slice):,} rows to {output_path}")
            else:
                # Group by both country and TNVED2
                grouped = df.group_by(['STRANA', 'TNVED2']).agg(pl.count())
                for row in grouped.iter_rows(named=True):
                    country_val = row['STRANA']
                    tnved2_val = row['TNVED2']
                    df_slice = df.filter(
                        (pl.col('STRANA') == country_val) & 
                        (pl.col('TNVED2') == tnved2_val)
                    )
                    filename = f"{country_val}_TNVED2_{tnved2_val}{file_ext}"
                    output_path = output_dir / filename
                    if output_format.lower() == 'excel':
                        df_slice.write_excel(output_path)
                    else:
                        df_slice.write_csv(output_path)
                    logger.info(f"Saved {len(df_slice):,} rows to {output_path}")
    
    finally:
        conn.close()


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Extract data slices from unified database by country and 2-digit TNVED code."
    )
    parser.add_argument(
        '--country',
        type=str,
        default=None,
        help="ISO2 country code to filter (e.g., CN, IN, TR). If not specified, extracts for all countries."
    )
    parser.add_argument(
        '--tnved2',
        type=str,
        default=None,
        help="2-digit TNVED code to filter (e.g., 01, 27). If not specified, extracts for all codes."
    )
    parser.add_argument(
        '--tnved4',
        type=str,
        default=None,
        help="4-digit TNVED code to filter (e.g., 2710). If specified, overrides --tnved2."
    )
    parser.add_argument(
        '--year',
        type=int,
        default=None,
        help="Year to filter (e.g., 2024). Extracts data from this year onwards."
    )
    parser.add_argument(
        '--db-path',
        type=str,
        default=None,
        help="Path to unified_trade_data.duckdb (default: db/unified_trade_data.duckdb)"
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default=None,
        help="Output directory for output files (default: data_interim_csv)"
    )
    parser.add_argument(
        '--format',
        type=str,
        choices=['csv', 'excel'],
        default='csv',
        help="Output format: 'csv' or 'excel' (default: csv)"
    )
    
    args = parser.parse_args()
    
    # Define paths
    project_root = Path(__file__).resolve().parent.parent
    db_path = Path(args.db_path) if args.db_path else project_root / "db" / "unified_trade_data.duckdb"
    output_dir = Path(args.output_dir) if args.output_dir else project_root / "data_interim_csv"
    
    logger.info("Starting extraction...")
    logger.info(f"Database: {db_path}")
    logger.info(f"Output directory: {output_dir}")
    if args.country:
        logger.info(f"Country filter: {args.country}")
    if args.tnved4:
        logger.info(f"TNVED4 filter: {args.tnved4}")
    elif args.tnved2:
        logger.info(f"TNVED2 filter: {args.tnved2}")
    if args.year:
        logger.info(f"Year filter: {args.year} onwards")
    logger.info(f"Output format: {args.format.upper()}")
    
    extract_slice(
        db_path=db_path,
        output_dir=output_dir,
        country=args.country,
        tnved2=args.tnved2,
        tnved4=args.tnved4,
        year=args.year,
        project_root=project_root,
        output_format=args.format
    )
    
    logger.info("Extraction completed")


if __name__ == "__main__":
    main()

