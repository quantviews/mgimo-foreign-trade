#!/usr/bin/env python3
"""
Script to merge Comtrade parquet files into a single DuckDB database.
Uses DuckDB's native parquet reading capabilities.
"""

import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Try to import duckdb
try:
    import duckdb
except ImportError:
    logger.error("DuckDB not available. Please install with: pip install duckdb")
    exit(1)

def get_parquet_files(data_dir: Path) -> list:
    """Get all parquet files from the comtrade data directory."""
    if not data_dir.exists():
        logger.warning(f"Data directory does not exist: {data_dir}")
        return []
    files = sorted(data_dir.glob("*.parquet"))
    return [str(f) for f in files]

def create_duckdb_database(parquet_files: list, db_path: Path) -> None:
    """
    Create DuckDB database from parquet files.
    
    Args:
        parquet_files: List of paths to parquet files
        db_path: Path to output DuckDB database file
    """
    logger.info(f"Creating DuckDB database at {db_path}")
    
    # Delete existing database file if it exists
    if db_path.exists():
        logger.info(f"Deleting existing database file: {db_path}")
        db_path.unlink()
    
    # Connect to DuckDB
    conn = duckdb.connect(str(db_path))
    
    try:
        # Create table from all parquet files
        logger.info(f"Processing {len(parquet_files)} parquet files...")
        
        # Build the UNION ALL query for all parquet files, filtering for detailed HS6 level data
        # Use DuckDB's safe parameter binding for file paths
        union_queries = []
        failed_files = []
        
        for file_path in parquet_files:
            try:
                # Use DuckDB's read_parquet with proper path handling
                # DuckDB handles paths safely when using read_parquet function
                file_path_str = str(Path(file_path).resolve())
                union_queries.append(f"""
                    SELECT 
                        refPeriodId, refYear, refMonth, 
                        -- Convert period from YYYYMM format to DATE
                        CAST(STRPTIME(CAST(period AS VARCHAR), '%Y%m') AS DATE) as period,
                        reporterCode, 
                        -- Reverse flowCode: M (Import from reporter's perspective) -> ЭК (Export from partner's perspective)
                        -- X (Export from reporter's perspective) -> ИМ (Import from partner's perspective)
                        CASE flowCode WHEN 'M' THEN 'ЭК' WHEN 'X' THEN 'ИМ' END as flowCode,
                        partnerCode, partner2Code, 
                        classificationCode, classificationSearchCode, isOriginalClassification, 
                        cmdCode, cmdDesc, aggrLevel, isLeaf, customsCode, 
                        mosCode, motCode, qtyUnitCode, qty, 
                        isQtyEstimated, altQtyUnitCode, altQtyUnitAbbr, altQty, 
                        netWgt, isNetWgtEstimated, grossWgt, isGrossWgtEstimated, 
                        cifvalue, fobvalue, primaryValue, legacyEstimationFlag, 
                        isReported, isAggregate
                    FROM read_parquet('{file_path_str.replace("'", "''")}') 
                    WHERE customsCode = 'C00' 
                      AND motCode = 0 
                      AND partner2Code = 0
                      AND LENGTH(CAST(cmdCode AS VARCHAR)) = 6
                """)
            except Exception as e:
                logger.warning(f"Failed to process file {file_path}: {e}")
                failed_files.append(file_path)
                continue
        
        if not union_queries:
            raise ValueError("No valid parquet files to process!")
        
        if failed_files:
            logger.warning(f"Failed to process {len(failed_files)} files. Continuing with {len(union_queries)} files.")
        
        union_query = " UNION ALL ".join(union_queries)
        create_query = f"CREATE TABLE comtrade_data AS {union_query}"
        
        logger.info("Executing merge query with filters: customsCode = 'C00', motCode = 0, partner2Code = 0, cmdCode length = 6, flowCode reversed...")
        conn.execute(create_query)
        
        # Get table info
        result = conn.execute("SELECT COUNT(*) as total_rows FROM comtrade_data").fetchone()
        row_count = result[0] if result else 0
        
        if row_count == 0:
            logger.warning("Table created but contains no rows!")
            return
        
        logger.info(f"Total rows in DuckDB (HS6 level, Russian perspective): {row_count:,}")
        
        # Check for potential duplicates and missing data
        logger.info("Checking for potential duplicates and data coverage...")
        
        # Check for duplicates by key fields
        duplicate_check = conn.execute("""
            SELECT 
                reporterCode, partnerCode, cmdCode, period, flowCode,
                COUNT(*) as record_count
            FROM comtrade_data
            GROUP BY reporterCode, partnerCode, cmdCode, period, flowCode
            HAVING COUNT(*) > 1
            ORDER BY record_count DESC
            LIMIT 10
        """).fetchall()
        
        if duplicate_check:
            logger.warning(f"Found {len(duplicate_check)} potential duplicate combinations!")
            logger.warning("Sample duplicates:")
            for row in duplicate_check[:5]:
                logger.warning(f"  Reporter: {row[0]}, Partner: {row[1]}, CMD: {row[2]}, Period: {row[3]}, Flow: {row[4]} - {row[5]} records")
        else:
            logger.info("No duplicates found - data is clean!")
        
        # Check data coverage by isReported status
        coverage_check = conn.execute("""
            SELECT 
                isReported,
                COUNT(*) as record_count,
                COUNT(DISTINCT reporterCode) as unique_reporters
            FROM comtrade_data
            GROUP BY isReported
        """).fetchall()
        
        logger.info("Data coverage by isReported status:")
        for row in coverage_check:
            status = "REPORTED" if row[0] else "ESTIMATED"
            logger.info(f"  {status}: {row[1]:,} records from {row[2]:,} reporters")
        
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
        indexes = [
            ("idx_refYear", "refYear"),
            ("idx_refMonth", "refMonth"),
            ("idx_reporterCode", "reporterCode"),
            ("idx_partnerCode", "partnerCode"),
            ("idx_flowCode", "flowCode"),
            ("idx_cmdCode", "cmdCode"),
            ("idx_period", "period"),
        ]
        
        created_indexes = 0
        for idx_name, col_name in indexes:
            try:
                # Check if index already exists
                conn.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON comtrade_data({col_name})")
                created_indexes += 1
            except Exception as e:
                logger.warning(f"Could not create index {idx_name} on {col_name}: {e}")
        
        logger.info(f"Created {created_indexes} out of {len(indexes)} indexes")
        
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
        
        if stats and stats[0] is not None:
            logger.info(f"Year range: {stats[0]} - {stats[1]}")
            logger.info(f"Unique reporters: {stats[2]:,}")
            logger.info(f"Unique partners: {stats[3]:,}")
            logger.info(f"Unique commodities: {stats[4]:,}")
        else:
            logger.warning("Could not retrieve statistics - table may be empty")
        
        # Get export and import sums by year
        logger.info("Getting export and import sums by year...")
        yearly_trade = conn.execute("""
            SELECT 
                refYear,
                flowCode,
                SUM(primaryValue) as total_value,
                COUNT(*) as record_count,
                COUNT(DISTINCT reporterCode) as unique_reporters
            FROM comtrade_data
            WHERE primaryValue IS NOT NULL AND primaryValue > 0
            GROUP BY refYear, flowCode
            ORDER BY refYear, flowCode
        """).fetchall()
        
        logger.info("=== TRADE VALUES BY YEAR ===")
        current_year = None
        for row in yearly_trade:
            year, flow_code, total_value, record_count, unique_reporters = row
            if year != current_year:
                logger.info(f"\nYear {year}:")
                current_year = year
            
            flow_name = "ЭКСПОРТ" if flow_code == 'ЭК' else "ИМПОРТ" if flow_code == 'ИМ' else f"FLOW_{flow_code}"
            logger.info(f"  {flow_name}: ${total_value:,.0f} ({record_count:,} records, {unique_reporters:,} reporters)")
        
        # Get total export and import sums
        total_trade = conn.execute("""
            SELECT 
                flowCode,
                SUM(primaryValue) as total_value,
                COUNT(*) as record_count,
                COUNT(DISTINCT reporterCode) as unique_reporters
            FROM comtrade_data
            WHERE primaryValue IS NOT NULL AND primaryValue > 0
            GROUP BY flowCode
            ORDER BY flowCode
        """).fetchall()
        
        logger.info("\n=== TOTAL TRADE VALUES ===")
        for row in total_trade:
            flow_code, total_value, record_count, unique_reporters = row
            flow_name = "ЭКСПОРТ" if flow_code == 'ЭК' else "ИМПОРТ" if flow_code == 'ИМ' else f"FLOW_{flow_code}"
            logger.info(f"{flow_name}: ${total_value:,.0f} ({record_count:,} records, {unique_reporters:,} reporters)")
        
    except Exception as e:
        logger.error(f"Error creating DuckDB database: {e}")
        raise
    finally:
        conn.close()

def main():
    """Main function to orchestrate the merge and conversion process."""
    # Define paths relative to script location
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    data_dir = project_root / "data_raw" / "comtrade_data"
    output_dir = project_root / "db"
    db_path = output_dir / "comtrade.db"
    
    # Create output directory if it doesn't exist
    output_dir.mkdir(exist_ok=True)
    
    # Get all parquet files
    parquet_files = get_parquet_files(data_dir)
    logger.info(f"Found {len(parquet_files)} parquet files in {data_dir}")
    
    if not parquet_files:
        logger.error(f"No parquet files found in {data_dir}!")
        return
    
    # Show first few files
    logger.info("First 5 files:")
    for i, file_path in enumerate(parquet_files[:5]):
        logger.info(f"  {i+1}. {Path(file_path).name}")
    
    # Create DuckDB database
    create_duckdb_database(parquet_files, db_path)
    
    logger.info("Process completed successfully!")
    logger.info(f"DuckDB database: {db_path}")
    if db_path.exists():
        logger.info(f"Database size: {db_path.stat().st_size / (1024*1024):.1f} MB")

if __name__ == "__main__":
    main()