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


# Paths and file patterns for original names per country (ISO2).
# Each loader returns Dict[8-digit HS code, commodity name] for lookup by TNVED[:8].
_COUNTRY_ORIGINAL_NAMES_CONFIG = {
    'TR': {
        'dir_relative': ['data_raw', 'turkey', 'hs_codes_json'],
        'glob': 'turkey_codes*.json',
        'loader': 'json_dict',  # JSON: { "hs8": "name", ... }
    },
    'CN': {
        'dir_relative': ['metadata', 'china'],
        'glob': '*-codes.json',
        'loader': 'china_json_list',  # JSON list: [ {"TNVED": "...", "COMMODITY_NAME": "..."}, ... ]
    },
    'IN': {
        'dir_relative': ['data_raw', 'india_new'],
        'glob': 'india_*.csv',
        'loader': 'india_csv',  # CSV: TNVED, Commodity
    },
}


def _load_original_names_tr(codes_dir: Path) -> Dict[str, str]:
    """Turkey: JSON dict HS8 -> name."""
    code_names = {}
    for json_file in sorted(codes_dir.glob("turkey_codes*.json")):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            for hs8_code, commodity_name in data.items():
                original_code = str(hs8_code).strip()
                if len(original_code) < 8:
                    original_code = original_code.zfill(8)
                elif len(original_code) > 8:
                    original_code = original_code[:8]
                if commodity_name:
                    code_names[original_code] = str(commodity_name).strip()
        except Exception as e:
            logger.error(f"Failed to process {json_file.name}: {e}")
    return code_names


def _load_original_names_cn(codes_dir: Path) -> Dict[str, str]:
    """China: JSON list with TNVED + COMMODITY_NAME, normalize to 10 then key by first 8."""
    code_names = {}
    for json_file in sorted(codes_dir.glob("*-codes.json")):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            for record in data:
                if not record.get('TNVED'):
                    continue
                original_code = str(record['TNVED']).strip()
                if len(original_code) == 10 and original_code.startswith('00'):
                    base_code = original_code[2:]
                else:
                    base_code = original_code.lstrip('0') or '0'
                tnved10 = base_code + '0' * (10 - len(base_code))
                hs8_key = tnved10[:8]
                if record.get('COMMODITY_NAME'):
                    code_names[hs8_key] = str(record['COMMODITY_NAME']).strip()
        except Exception as e:
            logger.error(f"Failed to process {json_file.name}: {e}")
    return code_names


def _load_original_names_in(codes_dir: Path) -> Dict[str, str]:
    """India: CSV with TNVED (8-digit), Commodity."""
    code_names = {}
    for csv_file in sorted(codes_dir.glob("india_*.csv")):
        try:
            df = pl.read_csv(csv_file, schema_overrides={'TNVED': pl.Utf8})
            if 'Commodity' not in df.columns:
                continue
            for row in df.iter_rows(named=True):
                original_code = str(row.get('TNVED', '')).strip()
                if not original_code:
                    continue
                if len(original_code) < 8:
                    original_code = original_code.zfill(8)
                elif len(original_code) > 8:
                    original_code = original_code[:8]
                name = row.get('Commodity')
                if name and str(name).strip():
                    code_names[original_code] = str(name).strip()
        except Exception as e:
            logger.error(f"Failed to process {csv_file.name}: {e}")
    return code_names


def load_country_original_names(country_iso2: str, project_root: Path) -> Dict[str, str]:
    """
    Load original commodity names for a country (8-digit HS code -> name).
    Supported: TR (Turkey), CN (China), IN (India). Others return {}.
    """
    country_iso2 = country_iso2.upper()
    config = _COUNTRY_ORIGINAL_NAMES_CONFIG.get(country_iso2)
    if not config:
        return {}
    codes_dir = project_root.joinpath(*config['dir_relative'])
    if not codes_dir.exists():
        logger.warning(f"Original names dir does not exist for {country_iso2}: {codes_dir}")
        return {}
    loader = config['loader']
    if loader == 'json_dict':
        code_names = _load_original_names_tr(codes_dir)
    elif loader == 'china_json_list':
        code_names = _load_original_names_cn(codes_dir)
    elif loader == 'india_csv':
        code_names = _load_original_names_in(codes_dir)
    else:
        return {}
    if code_names:
        logger.info(f"Loaded {len(code_names)} original names for {country_iso2}")
    return code_names


def extract_slice(
    db_path: Path,
    output_dir: Path,
    countries: Optional[list] = None,
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
        countries: List of ISO2 country codes to filter (if None or empty, extracts for all countries)
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
        
        if countries and len(countries) > 0:
            # Normalize country codes to uppercase
            countries_upper = [c.upper() for c in countries]
            if len(countries_upper) == 1:
                query += " AND STRANA = ?"
                params.append(countries_upper[0])
            else:
                # Use IN clause for multiple countries
                placeholders = ', '.join(['?' for _ in countries_upper])
                query += f" AND STRANA IN ({placeholders})"
                params.extend(countries_upper)
        
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
        
        # Add original names per country (TR, CN, IN, ...); for countries in filter or in data if no filter
        countries_list = [c.upper() for c in (countries or [])]
        if not countries_list:
            countries_list = sorted(df['STRANA'].unique().to_list())
        # Only try for countries we support
        countries_list = [c for c in countries_list if c in _COUNTRY_ORIGINAL_NAMES_CONFIG]
        if countries_list and project_root:
            for country_iso in countries_list:
                code_names = load_country_original_names(country_iso, project_root)
                col_name = f'TNVED_NAME_ORIGINAL_{country_iso}'
                if code_names:
                    def get_name(tnved_code: str, names: Dict[str, str]) -> str:
                        if not tnved_code or len(tnved_code) < 8:
                            return ''
                        return names.get(tnved_code[:8], '')

                    # Fill only for rows of this country
                    df = df.with_columns(
                        pl.when(pl.col('STRANA') == country_iso)
                        .then(pl.col('TNVED').map_elements(lambda c: get_name(c, code_names), return_dtype=pl.Utf8))
                        .otherwise(pl.lit(''))
                        .alias(col_name)
                    )
                    logger.info(f"Added original names column {col_name}")
                else:
                    df = df.with_columns(pl.lit('').alias(col_name))
        
        # Determine output filename and extension
        file_ext = '.xlsx' if output_format.lower() == 'excel' else '.csv'
        
        # Check if we have a single country and code combination
        has_countries = countries and len(countries) > 0
        has_code = tnved4 or tnved2
        
        if has_countries and len(countries) == 1 and has_code:
            # Single slice: one country and code specified
            country = countries[0].upper()
            if tnved4:
                code_part = f"TNVED4_{tnved4.zfill(4)}"
            else:
                code_part = f"TNVED2_{tnved2.zfill(2)}"
            year_part = f"_from_{year}" if year else ""
            filename = f"{country}_{code_part}{year_part}{file_ext}"
            output_path = output_dir / filename
            
            # Save based on format
            if output_format.lower() == 'excel':
                df.write_excel(output_path)
            else:
                df.write_csv(output_path)
            logger.info(f"Saved to {output_path}")
        elif has_countries and len(countries) > 1 and has_code:
            # Multiple countries, single code: create separate file for each country
            for country_val in sorted(df['STRANA'].unique().to_list()):
                df_slice = df.filter(pl.col('STRANA') == country_val)
                if tnved4:
                    code_part = f"TNVED4_{tnved4.zfill(4)}"
                else:
                    code_part = f"TNVED2_{tnved2.zfill(2)}"
                year_part = f"_from_{year}" if year else ""
                filename = f"{country_val}_{code_part}{year_part}{file_ext}"
                output_path = output_dir / filename
                if output_format.lower() == 'excel':
                    df_slice.write_excel(output_path)
                else:
                    df_slice.write_csv(output_path)
                logger.info(f"Saved {len(df_slice):,} rows to {output_path}")
        else:
            # Multiple slices: group by country and/or TNVED2
            if has_countries and len(countries) == 1:
                # Single country: group by TNVED2 only
                country = countries[0].upper()
                for tnved2_val in sorted(df['TNVED2'].unique().to_list()):
                    df_slice = df.filter(pl.col('TNVED2') == tnved2_val)
                    filename = f"{country}_TNVED2_{tnved2_val}{file_ext}"
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
        nargs='+',
        default=None,
        help="ISO2 country code(s) to filter (e.g., CN, or CN IN TR). Can specify multiple countries. If not specified, extracts for all countries."
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
        if len(args.country) == 1:
            logger.info(f"Country filter: {args.country[0]}")
        else:
            logger.info(f"Country filter: {', '.join(args.country)} ({len(args.country)} countries)")
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
        countries=args.country,
        tnved2=args.tnved2,
        tnved4=args.tnved4,
        year=args.year,
        project_root=project_root,
        output_format=args.format
    )
    
    logger.info("Extraction completed")


if __name__ == "__main__":
    main()

