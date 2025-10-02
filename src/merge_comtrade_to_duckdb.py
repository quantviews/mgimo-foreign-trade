#!/usr/bin/env python3
"""
Script to merge Comtrade parquet files into a single DuckDB database.
Uses DuckDB's native parquet reading capabilities.
"""

import os
import glob
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Try to import duckdb
try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    logger.error("DuckDB not available. Please install with: pip install duckdb")
    DUCKDB_AVAILABLE = False
    exit(1)

def get_parquet_files(data_dir: str) -> list:
    """Get all parquet files from the comtrade data directory."""
    pattern = os.path.join(data_dir, "*.parquet")
    files = glob.glob(pattern)
    return sorted(files)

def create_duckdb_database(parquet_files: list, db_path: str) -> None:
    """Create DuckDB database from parquet files."""
    logger.info(f"Creating DuckDB database at {db_path}")
    
    # Delete existing database file if it exists
    if os.path.exists(db_path):
        logger.info(f"Deleting existing database file: {db_path}")
        os.remove(db_path)
    
    # Connect to DuckDB
    conn = duckdb.connect(db_path)
    
    try:
        # Create table from all parquet files
        logger.info("Creating table from parquet files...")
        
        # Build the UNION ALL query for all parquet files, filtering for isReported == True
        # Convert period column to Date format
        union_queries = []
        for file_path in parquet_files:
            # Escape single quotes in file path for SQL
            escaped_path = file_path.replace("'", "''")
            union_queries.append(f"""
                SELECT 
                    refPeriodId, refYear, refMonth, 
                    CAST(SUBSTR(CAST(period AS VARCHAR), 1, 4) || '-' || SUBSTR(CAST(period AS VARCHAR), 5, 2) || '-01' AS DATE) as period,
                    reporterCode, flowCode, 
                    partnerCode, partner2Code, 
                    classificationCode, classificationSearchCode, isOriginalClassification, 
                    cmdCode, cmdDesc, aggrLevel, isLeaf, customsCode, 
                    mosCode, motCode, qtyUnitCode, qty, 
                    isQtyEstimated, altQtyUnitCode, altQtyUnitAbbr, altQty, 
                    netWgt, isNetWgtEstimated, grossWgt, isGrossWgtEstimated, 
                    cifvalue, fobvalue, primaryValue, legacyEstimationFlag, 
                    isReported, isAggregate
                FROM read_parquet('{escaped_path}') 
                WHERE customsCode = 'C00' AND motCode = 0 AND partner2Code = 0
            """)
        
        union_query = " UNION ALL ".join(union_queries)
        create_query = f"CREATE TABLE comtrade_data AS {union_query}"
        
        logger.info("Executing merge query (customsCode = 'C00' AND motCode = 0 AND partner2Code = 0)...")
        conn.execute(create_query)
        
        # Get table info
        result = conn.execute("SELECT COUNT(*) as total_rows FROM comtrade_data").fetchone()
        logger.info(f"Total rows in DuckDB (customsCode = 'C00' AND motCode = 0 AND partner2Code = 0): {result[0]:,}")
        
        # Get column info
        columns_info = conn.execute("DESCRIBE comtrade_data").fetchall()
        logger.info(f"Columns in DuckDB table: {len(columns_info)}")
        for col in columns_info:
            logger.info(f"  {col[0]}: {col[1]}")
        
        # Get sample data
        sample_data = conn.execute("SELECT * FROM comtrade_data LIMIT 5").fetchall()
        logger.info("Sample data:")
        for row in sample_data:
            logger.info(f"  {row}")
        
        # Create indexes for common query patterns based on Comtrade schema
        logger.info("Creating indexes...")
        try:
            conn.execute("CREATE INDEX idx_refYear ON comtrade_data(refYear)")
            conn.execute("CREATE INDEX idx_refMonth ON comtrade_data(refMonth)")
            conn.execute("CREATE INDEX idx_reporterCode ON comtrade_data(reporterCode)")
            conn.execute("CREATE INDEX idx_partnerCode ON comtrade_data(partnerCode)")
            conn.execute("CREATE INDEX idx_flowCode ON comtrade_data(flowCode)")
            conn.execute("CREATE INDEX idx_cmdCode ON comtrade_data(cmdCode)")
            logger.info("Indexes created successfully")
        except Exception as e:
            logger.warning(f"Could not create indexes: {e}")
        
        # Get some basic statistics
        logger.info("Getting basic statistics...")
        stats = conn.execute("""
            SELECT 
                MIN(refYear) as min_year,
                MAX(refYear) as max_year,
                COUNT(DISTINCT reporterCode) as unique_reporters,
                COUNT(DISTINCT partnerCode) as unique_partners,
                COUNT(DISTINCT cmdCode) as unique_commodities
            FROM comtrade_data
        """).fetchone()
        
        logger.info(f"Year range: {stats[0]} - {stats[1]}")
        logger.info(f"Unique reporters: {stats[2]:,}")
        logger.info(f"Unique partners: {stats[3]:,}")
        logger.info(f"Unique commodities: {stats[4]:,}")
        
    except Exception as e:
        logger.error(f"Error creating DuckDB database: {e}")
        raise
    finally:
        conn.close()

def main():
    """Main function to orchestrate the merge and conversion process."""
    # Define paths
    data_dir = "data_raw/comtrade_data"
    output_dir = "db"
    db_path = os.path.join(output_dir, "comtrade.db")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Get all parquet files
    parquet_files = get_parquet_files(data_dir)
    logger.info(f"Found {len(parquet_files)} parquet files")
    
    if not parquet_files:
        logger.error("No parquet files found!")
        return
    
    # Show first few files
    logger.info("First 5 files:")
    for i, file_path in enumerate(parquet_files[:5]):
        logger.info(f"  {i+1}. {os.path.basename(file_path)}")
    
    # Create DuckDB database
    create_duckdb_database(parquet_files, db_path)
    
    logger.info("Process completed successfully!")
    logger.info(f"DuckDB database: {db_path}")
    logger.info(f"Database size: {os.path.getsize(db_path) / (1024*1024):.1f} MB")

if __name__ == "__main__":
    main()