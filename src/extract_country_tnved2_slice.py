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
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def extract_slice(
    db_path: Path,
    output_dir: Path,
    country: Optional[str] = None,
    tnved2: Optional[str] = None
) -> None:
    """
    Extract data slice from unified database.
    
    Args:
        db_path: Path to unified_trade_data.duckdb
        output_dir: Directory to save CSV files
        country: ISO2 country code to filter (if None, extracts for all countries)
        tnved2: 2-digit TNVED code to filter (if None, extracts for all codes)
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
        
        if tnved2:
            query += " AND TNVED2 = ?"
            params.append(tnved2.zfill(2))  # Ensure 2 digits with leading zeros
        
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
        
        # Determine output filename
        if country and tnved2:
            # Single slice: country and code specified
            filename = f"{country.upper()}_TNVED2_{tnved2.zfill(2)}.csv"
            output_path = output_dir / filename
            df.write_csv(output_path)
            logger.info(f"Saved to {output_path}")
        else:
            # Multiple slices: group by country and/or TNVED2
            if country:
                # Group by TNVED2 only
                for tnved2_val in sorted(df['TNVED2'].unique().to_list()):
                    df_slice = df.filter(pl.col('TNVED2') == tnved2_val)
                    filename = f"{country.upper()}_TNVED2_{tnved2_val}.csv"
                    output_path = output_dir / filename
                    df_slice.write_csv(output_path)
                    logger.info(f"Saved {len(df_slice):,} rows to {output_path}")
            elif tnved2:
                # Group by country only
                for country_val in sorted(df['STRANA'].unique().to_list()):
                    df_slice = df.filter(pl.col('STRANA') == country_val)
                    filename = f"{country_val}_TNVED2_{tnved2.zfill(2)}.csv"
                    output_path = output_dir / filename
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
                    filename = f"{country_val}_TNVED2_{tnved2_val}.csv"
                    output_path = output_dir / filename
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
        '--db-path',
        type=str,
        default=None,
        help="Path to unified_trade_data.duckdb (default: db/unified_trade_data.duckdb)"
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default=None,
        help="Output directory for CSV files (default: data_interim_csv)"
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
    if args.tnved2:
        logger.info(f"TNVED2 filter: {args.tnved2}")
    
    extract_slice(
        db_path=db_path,
        output_dir=output_dir,
        country=args.country,
        tnved2=args.tnved2
    )
    
    logger.info("Extraction completed")


if __name__ == "__main__":
    main()

