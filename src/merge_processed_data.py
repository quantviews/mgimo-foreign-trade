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
    'PERIOD': 'datetime64[ns]', # DATE - отчетный период (normalized to 00:00:00, DuckDB will save as DATE)
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
            # Validate schema
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

def generate_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate derived TNVED columns if they don't exist or validate them.
    
    Args:
        df: DataFrame to process
        
    Returns:
        DataFrame with validated/generated derived columns
    """


    df_processed = df.copy()
    
    # Ensure TNVED columns are strings and generate derived columns
    if 'TNVED' in df_processed.columns:
        # Convert TNVED to string
        df_processed['TNVED'] = df_processed['TNVED'].astype(str).str.strip()
        
        # Pad TNVED to 10 digits on the RIGHT if needed (never remove leading zeros)
        def pad_right(code):
            if len(code) >= 10:
                return code[:10]
            return code + '0' * (10 - len(code))
        df_processed['TNVED'] = df_processed['TNVED'].apply(pad_right)
        
        # Generate derived columns from TNVED (preserving leading zeros)
        df_processed['TNVED2'] = df_processed['TNVED'].str[:2]
        df_processed['TNVED4'] = df_processed['TNVED'].str[:4]
        df_processed['TNVED6'] = df_processed['TNVED'].str[:6]
        df_processed['TNVED8'] = df_processed['TNVED'].str[:8]
            
    return df_processed


def transform_fizob_to_unified(df: pd.DataFrame, file_stem: str) -> pd.DataFrame:
    """
    Transform a fizob parquet DataFrame to unified fizob_index schema.
    Schema: STRANA, NAPR, PERIOD, tn_level, tn_code, fizob, fizob_bp
    """
    df = df.copy()
    if 'PERIOD' in df.columns:
        df['PERIOD'] = pd.to_datetime(df['PERIOD'], errors='coerce').dt.normalize()
    
    if file_stem == 'fizob_total':
        # Total level: aggregated across all TNVED, tn_level=0, tn_code='0'
        if 'fizob' not in df.columns or 'fizob_bp' not in df.columns:
            logger.warning(f"fizob_total missing fizob/fizob_bp columns, skipping")
            return pd.DataFrame()
        out = df[['STRANA', 'NAPR', 'PERIOD']].copy()
        out['tn_level'] = 0
        out['tn_code'] = df['TNVED2'].fillna(0).astype(int).astype(str) if 'TNVED2' in df.columns else '0'
        out['fizob'] = df['fizob'].values
        out['fizob_bp'] = df['fizob_bp'].values
        return out
    
    # Level-specific: fizob_2, fizob_4, fizob_6
    mapping = {
        'fizob_2': (2, 'TNVED2', 'fizob2', 'fizob2_bp'),
        'fizob2': (2, 'TNVED2', 'fizob2', 'fizob2_bp'),
        'fizob_4': (4, 'TNVED4', 'fizob4', 'fizob4_bp'),
        'fizob4': (4, 'TNVED4', 'fizob4', 'fizob4_bp'),
        'fizob_6': (6, 'TNVED6', 'fizob6', 'fizob6_bp'),
        'fizob6': (6, 'TNVED6', 'fizob6', 'fizob6_bp'),
    }
    if file_stem not in mapping:
        logger.warning(f"Unknown fizob file stem '{file_stem}', skipping")
        return pd.DataFrame()
    
    level, tnved_col, fizob_col, fizob_bp_col = mapping[file_stem]
    for col in [tnved_col, fizob_col, fizob_bp_col]:
        if col not in df.columns:
            logger.warning(f"{file_stem} missing column {col}, skipping")
            return pd.DataFrame()
    
    out = df[['STRANA', 'NAPR', 'PERIOD', tnved_col]].copy()
    out = out.rename(columns={tnved_col: 'tn_code'})
    out['tn_level'] = level
    out['fizob'] = df[fizob_col].values
    out['fizob_bp'] = df[fizob_bp_col].values
    return out


def save_to_duckdb(df: pd.DataFrame, output_path: Path, table_name: str = 'unified_trade_data', chunk_size: int = 100000):
    """
    Save DataFrame to DuckDB database in chunks to conserve memory.
    
    Args:
        df: DataFrame to save
        output_path: Path to DuckDB file
        table_name: Name of the table in database
        chunk_size: Number of rows to write per chunk
    """
    logger.info(f"Saving merged data to DuckDB: {output_path}")

    # It's safer to delete the old DB file to ensure a clean write.
    if output_path.exists():
        output_path.unlink()

    if df.empty:
        logger.warning("Input DataFrame is empty. Nothing to save to DuckDB.")
        return

    try:
        conn = duckdb.connect(str(output_path))
        
        # Ensure PERIOD is normalized (time set to 00:00:00) before saving
        # We'll cast it to DATE in DuckDB to remove time component completely
        if 'PERIOD' in df.columns:
            # Convert to datetime and normalize to remove time (set to 00:00:00)
            df['PERIOD'] = pd.to_datetime(df['PERIOD'], errors='coerce').dt.normalize()
        
        # Create the table and insert the first chunk
        # Explicitly cast PERIOD to DATE in DuckDB to ensure no time component
        first_chunk = df.iloc[:chunk_size]
        conn.register('first_chunk_df', first_chunk)
        if 'PERIOD' in first_chunk.columns:
            conn.execute(f"""
                CREATE TABLE {table_name} AS 
                SELECT 
                    * EXCLUDE (PERIOD),
                    CAST(PERIOD AS DATE) AS PERIOD
                FROM first_chunk_df
            """)
        else:
            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM first_chunk_df")
        conn.unregister('first_chunk_df')
        logger.info(f"  ... created table and inserted first {len(first_chunk):,} rows")
        logger.info(f"  ... PERIOD column saved as DATE type (no time component)")

        # Insert the rest of the data in chunks using the efficient append method
        for i in range(chunk_size, len(df), chunk_size):
            chunk = df.iloc[i:i + chunk_size].copy()  # Explicit copy to avoid SettingWithCopyWarning
            # Ensure PERIOD is normalized before appending
            if 'PERIOD' in chunk.columns:
                chunk['PERIOD'] = pd.to_datetime(chunk['PERIOD'], errors='coerce').dt.normalize()
            # Use INSERT with explicit DATE cast for PERIOD to ensure no time component
            if 'PERIOD' in chunk.columns:
                conn.register('chunk_df', chunk)
                conn.execute(f"""
                    INSERT INTO {table_name}
                    SELECT 
                        * EXCLUDE (PERIOD),
                        CAST(PERIOD AS DATE) AS PERIOD
                    FROM chunk_df
                """)
                conn.unregister('chunk_df')
            else:
                conn.append(table_name, chunk)
            logger.info(f"  ... inserted {i + len(chunk):,} / {len(df):,} rows")

        # Get row count for verification
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        row_count = result[0]
        
        conn.close()

        if row_count != len(df):
            logger.warning(f"Row count mismatch! Expected {len(df):,}, but DuckDB table has {row_count:,}.")
        
        logger.info(f"Successfully saved {row_count:,} rows to {output_path}")
        
    except Exception as e:
        logger.error(f"Failed to save to DuckDB: {e}")
        raise

def save_reference_tables(conn: duckdb.DuckDBPyConnection, project_root: Path):
    """
    Save reference tables (TNVED names, country names) as separate tables in DuckDB.
    This normalizes the database structure and reduces data duplication.
    
    Args:
        conn: DuckDB connection
        project_root: Path to project root for metadata loading
    """
    logger.info("Creating reference tables...")
    
    # Save TNVED mappings
    tnved_mappings = load_tnved_mapping(project_root)
    if tnved_mappings:
        # Create unified TNVED reference table
        tnved_refs = []
        for level_name, mapping in tnved_mappings.items():
            # Extract level number from key like 'tnved2', 'tnved10', etc.
            level_num = level_name.replace('tnved', '').replace('TNVED', '')
            try:
                level_int = int(level_num)
            except ValueError:
                logger.warning(f"Could not parse TNVED level from '{level_name}', skipping...")
                continue
            for code, code_data in mapping.items():
                # code_data is now a dict with 'name' and 'translated' keys
                name = code_data.get('name', '')
                translated = code_data.get('translated', False)
                
                if not name:
                    continue
                
                # Prepare code to match format in unified_trade_data
                # IMPORTANT: For ALL levels (2, 4, 6, 8, 10) - codes should match original structure (with leading zeros)
                # No normalization (removal of leading zeros) should be applied
                code_str = str(code).strip()
                
                # For all levels: use original code structure (with leading zeros)
                # First ensure code is at least 10 digits by padding with zeros on the RIGHT if needed
                if len(code_str) >= 10:
                    code_padded = code_str[:10]
                else:
                    code_padded = code_str + '0' * (10 - len(code_str))
                
                # Extract the appropriate length for this level (from original structure, preserving leading zeros)
                if level_int == 2:
                    normalized_code = code_padded[:2]
                elif level_int == 4:
                    normalized_code = code_padded[:4]
                elif level_int == 6:
                    normalized_code = code_padded[:6]
                elif level_int == 8:
                    normalized_code = code_padded[:8]
                elif level_int == 10:
                    normalized_code = code_padded[:10]
                else:
                    normalized_code = code_str
                
                tnved_refs.append({
                    'TNVED_CODE': normalized_code,
                    'TNVED_LEVEL': level_int,
                    'TNVED_NAME': name,
                    'TRANSLATED': translated
                })
        
        if tnved_refs:
            tnved_df = pd.DataFrame(tnved_refs)
            # Remove duplicates, keeping official mappings (translated=False) over translations (translated=True)
            # Sort so that translated=False comes first, then drop duplicates
            tnved_df = tnved_df.sort_values('TRANSLATED').drop_duplicates(
                subset=['TNVED_CODE', 'TNVED_LEVEL'],
                keep='first'
            )
            
            conn.register('tnved_ref_df', tnved_df)
            conn.execute("""
                CREATE TABLE tnved_reference AS 
                SELECT DISTINCT TNVED_CODE, TNVED_LEVEL, TNVED_NAME, TRANSLATED
                FROM tnved_ref_df
                ORDER BY TNVED_LEVEL, TNVED_CODE
            """)
            conn.unregister('tnved_ref_df')
            
            official_count = (tnved_df['TRANSLATED'] == False).sum()
            translated_count = (tnved_df['TRANSLATED'] == True).sum()
            logger.info(f"  ... created tnved_reference table with {len(tnved_df)} rows "
                       f"({official_count} official, {translated_count} translated)")
            
            # Create index for faster joins
            conn.execute("CREATE INDEX idx_tnved_ref_code_level ON tnved_reference(TNVED_CODE, TNVED_LEVEL)")
    
    # Save country name mappings
    strana_mapping = load_strana_mapping(project_root)
    if strana_mapping:
        country_refs = [{'STRANA': k, 'STRANA_NAME': v} for k, v in strana_mapping.items()]
        country_df = pd.DataFrame(country_refs)
        conn.register('country_ref_df', country_df)
        conn.execute("""
            CREATE TABLE country_reference AS 
            SELECT DISTINCT STRANA, STRANA_NAME
            FROM country_ref_df
            ORDER BY STRANA
        """)
        conn.unregister('country_ref_df')
        logger.info(f"  ... created country_reference table with {len(country_df)} rows")
        
        # Create index for faster joins
        conn.execute("CREATE INDEX idx_country_ref_strana ON country_reference(STRANA)")
    
    # Create convenience view that joins main table with reference tables
    logger.info("Creating convenience view with joined reference data...")
    conn.execute("""
        CREATE OR REPLACE VIEW unified_trade_data_enriched AS
        SELECT 
            t.*,
            c.STRANA_NAME AS COUNTRY_NAME,
            t2.TNVED_NAME AS TNVED2_NAME,
            t4.TNVED_NAME AS TNVED4_NAME,
            t6.TNVED_NAME AS TNVED6_NAME,
            t8.TNVED_NAME AS TNVED8_NAME,
            COALESCE(t10.TNVED_NAME, t8.TNVED_NAME) AS TNVED_NAME,
            COALESCE(t10.TRANSLATED, t8.TRANSLATED) AS TNVED_TRANSLATED,
            DENSE_RANK() OVER (
                PARTITION BY t.STRANA, t.TNVED, t.NAPR
                ORDER BY t.PERIOD DESC
            ) AS period_rank
        FROM unified_trade_data t
        LEFT JOIN country_reference c ON t.STRANA = c.STRANA
        LEFT JOIN tnved_reference t2 ON t.TNVED2 = t2.TNVED_CODE AND t2.TNVED_LEVEL = 2
        LEFT JOIN tnved_reference t4 ON t.TNVED4 = t4.TNVED_CODE AND t4.TNVED_LEVEL = 4
        LEFT JOIN tnved_reference t6 ON t.TNVED6 = t6.TNVED_CODE AND t6.TNVED_LEVEL = 6
        LEFT JOIN tnved_reference t8 ON t.TNVED8 = t8.TNVED_CODE AND t8.TNVED_LEVEL = 8
        LEFT JOIN tnved_reference t10 ON t.TNVED = t10.TNVED_CODE AND t10.TNVED_LEVEL = 10
    """)
    logger.info("  ... created unified_trade_data_enriched view")

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
    """Loads a comprehensive, case-insensitive mapping for EDIZM values."""
    mapping_file = project_root / "metadata" / "edizm.csv"
    if not mapping_file.exists():
        logger.error(f"Common EDIZM mapping file not found at {mapping_file}")
        return {}

    try:
        # Read all columns as strings and prevent pandas from interpreting "NA" as NaN
        df = pd.read_csv(mapping_file, dtype=str, na_filter=False)
        
        # Standardize column names and values to uppercase for case-insensitive matching
        df.columns = df.columns.str.upper()
        df['KOD'] = df['KOD'].str.replace('"', '').str.strip()
        df['NAME'] = df['NAME'].str.upper().str.strip()

        # Create canonical records from the main edizm file (vectorized)
        canonical_records = {}
        # Use itertuples for better performance than iterrows
        for row in df.itertuples(index=False):
            record = {'KOD': row.KOD, 'NAME': row.NAME}
            canonical_records[row.NAME] = record
            # Also map by KOD if it exists
            if row.KOD:
                canonical_records[row.KOD] = record

        final_mapping = {}
        # Populate mapping from the edizm file itself (KOD, NAME) - vectorized
        for row in df.itertuples(index=False):
            record = canonical_records[row.NAME]
            final_mapping[row.NAME] = record
            if row.KOD:
                final_mapping[row.KOD] = record
        
        # Add a comprehensive set of aliases. All keys must be uppercase.
        aliases = {
            # Russian abbreviations
            'ШТ': canonical_records.get('ШТУКА'),
            'КГ': canonical_records.get('КИЛОГРАММ'),
            'Т': canonical_records.get('ТОННА, МЕТРИЧЕСКАЯ ТОННА (1000 КГ)'),
            'М': canonical_records.get('МЕТР'),
            'М2': canonical_records.get('КВАДРАТНЫЙ МЕТР'),
            'М3': canonical_records.get('КУБИЧЕСКИЙ МЕТР'),
            'Л': canonical_records.get('ЛИТР'),
            'Г': canonical_records.get('ГРАММ'),
            'КАРАТ': canonical_records.get('МЕТРИЧЕСКИЙ КАРАТ(1КАРАТ=2*10(-4)КГ'),

            # Comtrade abbreviations (from comtradte-QuantityUnits.json)
            'KG': canonical_records.get('КИЛОГРАММ'),
            'U': canonical_records.get('ШТУКА'),
            'L': canonical_records.get('ЛИТР'),
            'M': canonical_records.get('МЕТР'),  # Latin M (meter)
            'M²': canonical_records.get('КВАДРАТНЫЙ МЕТР'),
            'M2': canonical_records.get('КВАДРАТНЫЙ МЕТР'),  # M2 without superscript
            'M3': canonical_records.get('КУБИЧЕСКИЙ МЕТР'),
            '2U': canonical_records.get('ПАРА'),
            'CARAT': canonical_records.get('МЕТРИЧЕСКИЙ КАРАТ(1КАРАТ=2*10(-4)КГ'),
            '1000U': canonical_records.get('ТЫСЯЧА ШТУК'),
            'G': canonical_records.get('ГРАММ'),
            '1000 KWH': canonical_records.get('1000 КИЛОВАТТ-ЧАС'),
            '1000 L': canonical_records.get('1000 ЛИТРОВ'),
            '1000 KG': canonical_records.get('ТОННА, МЕТРИЧЕСКАЯ ТОННА (1000 КГ)'),
            'L ALC 100%': canonical_records.get('ЛИТР ЧИСТОГО (100%) СПИРТА'),
            # Additional Comtrade codes
            'BBL': canonical_records.get('БАРРЕЛЬ'),  # Barrel (code 11, if exists)
            'CT/L': canonical_records.get('ТОННА ГРУЗОПОДЪЕМНОСТИ'),  # Carrying capacity in tonnes (code 36, if exists)
            '12U': canonical_records.get('ШТУКА'),  # 12 units (approximate to piece)
            'KG/NET EDA': canonical_records.get('КИЛОГРАММ'),  # Variant with / instead of space
            'KG MET.AM.': canonical_records.get('КИЛОГРАММ МЕТАЛЛИЧЕСКОГО АММИАКА'),  # Kilogram of metallic ammonium (if exists)
            'GI F/S': canonical_records.get('ГРАММ ДЕЛЯЩИХСЯ ИЗОТОПОВ'),  # Gram of fissile isotopes (code 38, if exists)
            'U (JEU/PACK)': canonical_records.get('УПАКОВКА') or canonical_records.get('ШТУКА'),  # Number of packages (code 10, if exists)
            'U JEU/PACK': canonical_records.get('УПАКОВКА') or canonical_records.get('ШТУКА'),  # Number of packages (without parentheses)
            'KG U': canonical_records.get('КИЛОГРАММ УРАНА'),  # Kilogram of uranium (code 35)
            'GT': canonical_records.get('ВАЛОВАЯ РЕГИСТРОВАЯ ВМЕСТИМОСТЬ'),  # Gross tonnage (code 40, if exists)
            'GRT': canonical_records.get('ВАЛОВАЯ РЕГИСТРОВАЯ ВМЕСТИМОСТЬ'),  # Gross register ton (code 39, if exists)

            # Other observed values from logs
            'KG NET EDA': canonical_records.get('КИЛОГРАММ'),
            'Л 100% СПИРТА': canonical_records.get('ЛИТР ЧИСТОГО (100%) СПИРТА'),
            'КГ NAOH': canonical_records.get('КИЛОГРАММ ГИДРОКСИДА НАТРИЯ'),
            'КГ KOH': canonical_records.get('КИЛОГРАММ ГИДРОКСИДА КАЛИЯ'),
            'КГ N': canonical_records.get('КИЛОГРАММ АЗОТА'),
            'КГ K2O': canonical_records.get('КИЛОГРАММ ОКСИДА КАЛИЯ'),
            'КГ P2O5': canonical_records.get('КИЛОГРАММ ПЯТИОКИСИ ФОСФОРА'),
            'КГ H2O2': canonical_records.get('КИЛОГРАММ ПЕРОКСИДА ВОДОРОДА'),
            'КГ 90 %-ГО СУХОГО ВЕЩЕСТВА': canonical_records.get('КИЛОГРАММ 90 %-ГО СУХОГО ВЕЩЕСТВА'),
            'КГ U': canonical_records.get('КИЛОГРАММ УРАНА'),
            
            # Additional variants and common misspellings
            'M³': canonical_records.get('КУБИЧЕСКИЙ МЕТР'),  # Superscript 3
            'M3': canonical_records.get('КУБИЧЕСКИЙ МЕТР'),  # Already exists, but ensure it's there
            'М³': canonical_records.get('КУБИЧЕСКИЙ МЕТР'),  # Cyrillic with superscript
            'KG H2O2': canonical_records.get('КИЛОГРАММ ПЕРОКСИДА ВОДОРОДА'),  # Latin version
            'KG N': canonical_records.get('КИЛОГРАММ АЗОТА'),  # Latin version
            'КГ 90% С/В': canonical_records.get('КИЛОГРАММ 90 %-ГО СУХОГО ВЕЩЕСТВА'),  # Variant with /В
            'КГ 90% СВ': canonical_records.get('КИЛОГРАММ 90 %-ГО СУХОГО ВЕЩЕСТВА'),  # Variant without /
            '1000 ШТ': canonical_records.get('ТЫСЯЧА ШТУК'),  # Russian version
            '100 ШТ': canonical_records.get('ШТУКА'),  # 100 pieces = pieces (approximate)
            'КАР': canonical_records.get('МЕТРИЧЕСКИЙ КАРАТ(1КАРАТ=2*10(-4)КГ'),  # Abbreviation
            'ЭЛЕМ': canonical_records.get('ШТУКА'),  # Element/piece (approximate)
            
            # Radioactivity units
            'BQ': canonical_records.get('БЕККЕРЕЛЬ'),  # Becquerel (if exists in edizm.csv)
            'BECQUEREL': canonical_records.get('БЕККЕРЕЛЬ'),  # Full name (if exists) - fixed: БЕККЕРЕЛЬ with double К
            'MILLION BQ': canonical_records.get('МИЛЛИОН БЕККЕРЕЛЕЙ'),  # Million Becquerels (if exists)
            
            # Additional Comtrade abbreviations and variants
            'KG NAOH': canonical_records.get('КИЛОГРАММ ГИДРОКСИДА НАТРИЯ'),  # Sodium hydroxide
            'KG KOH': canonical_records.get('КИЛОГРАММ ГИДРОКСИДА КАЛИЯ'),  # Potassium hydroxide
            'KG K2O': canonical_records.get('КИЛОГРАММ ОКСИДА КАЛИЯ'),  # Potassium oxide
            'KG P2O5': canonical_records.get('КИЛОГРАММ ПЯТИОКИСИ ФОСФОРА'),  # Phosphorus pentoxide
            'KG 90% SDT': canonical_records.get('КИЛОГРАММ 90 %-ГО СУХОГО ВЕЩЕСТВА'),  # 90% dry substance (SDT variant)
            'KG 90% SD': canonical_records.get('КИЛОГРАММ 90 %-ГО СУХОГО ВЕЩЕСТВА'),  # 90% dry substance (SD variant)
            '1000 M3': canonical_records.get('1000 КУБИЧЕСКИХ МЕТРОВ'),  # 1000 cubic meters (if exists)
            'CE/EL': canonical_records.get('ЭЛЕМЕНТ') or canonical_records.get('ШТУКА'),  # Number of cells/elements
            'CE EL': canonical_records.get('ЭЛЕМЕНТ') or canonical_records.get('ШТУКА'),  # Number of cells/elements (space variant)
            'TJ': canonical_records.get('ТЕРАДЖОУЛЬ'),  # Terajoule (if exists)
            'TERAJOULE': canonical_records.get('ТЕРАДЖОУЛЬ'),  # Terajoule full name (if exists)
        }
        
        final_mapping.update(aliases)
        
        # Filter out any None values that may have resulted from missing keys
        final_mapping = {k: v for k, v in final_mapping.items() if v is not None and k is not None}

        # Diagnostic: Check if БЕККЕРЕЛЬ was loaded
        if 'БЕККЕРЕЛЬ' in canonical_records:
            logger.info(f"  - 'БЕККЕРЕЛЬ' found in canonical_records")
            if 'BQ' in final_mapping:
                logger.info(f"  - 'BQ' alias successfully added to final_mapping")
            else:
                logger.warning(f"  - 'BQ' alias NOT found in final_mapping (canonical_records.get returned None)")
        else:
            logger.warning(f"  - 'БЕККЕРЕЛЬ' NOT found in canonical_records")

        logger.info(f"Loaded common EDIZM mapping with {len(final_mapping)} case-insensitive keys.")
        return final_mapping
    except Exception as e:
        logger.error(f"Failed to load common EDIZM mapping: {e}")
        return {}

def load_strana_mapping(project_root: Path) -> Dict[str, str]:
    """Loads ISO2 to country name mapping from STRANA.csv."""
    mapping_file = project_root / "metadata" / "STRANA.csv"
    if not mapping_file.exists():
        logger.error(f"Country name mapping file not found at {mapping_file}")
        return {}
    
    try:
        # Assuming the separator is a tab.
        df = pd.read_csv(mapping_file, sep='	', dtype=str)
        df.columns = df.columns.str.upper()
        # Create case-insensitive mapping: uppercase KOD (ISO2) -> NAME
        mapping = pd.Series(df.NAME.values, index=df.KOD.str.upper()).to_dict()
        logger.info(f"Loaded country name mapping for {len(mapping)} countries.")
        return mapping
    except Exception as e:
        logger.error(f"Failed to load country name mapping: {e}")
        return {}

def load_tnved_mapping(project_root: Path) -> Dict[str, Dict[str, Dict[str, any]]]:
    """
    Loads TNVED code to name mappings from tnved.csv and missing_codes_translations.json.
    
    Returns a dictionary with structure:
    {
        'tnved2': {code: {'name': str, 'translated': bool}},
        'tnved4': {code: {'name': str, 'translated': bool}},
        ...
    }
    """
    mapping_file = project_root / "metadata" / "tnved.csv"
    translations_file = project_root / "metadata" / "translations" / "missing_codes_translations.json"
    
    # Initialize mappings structure
    mappings = {
        'tnved2': {},
        'tnved4': {},
        'tnved6': {},
        'tnved8': {},
        'tnved10': {}
    }
    
    # Load official mappings from tnved.csv
    if mapping_file.exists():
        try:
            df = pd.read_csv(mapping_file, dtype={'KOD': str, 'NAME': str, 'level': int})
            df.columns = df.columns.str.upper()

            for level in [2, 4, 6, 8, 10]:
                level_key = f'tnved{level}'
                level_data = df[df['LEVEL'] == level]
                for _, row in level_data.iterrows():
                    code = str(row['KOD']).strip()
                    name = str(row['NAME']).strip().upper()
                    mappings[level_key][code] = {
                        'name': name,
                        'translated': False
                    }
            
            logger.info("Successfully loaded official TNVED mappings for all levels.")
        except Exception as e:
            logger.error(f"Failed to load TNVED mapping from {mapping_file}: {e}")
    else:
        logger.warning(f"TNVED mapping file not found at {mapping_file}")
    
    # Load translations from missing_codes_translations_test.json
    if translations_file.exists():
        try:
            with open(translations_file, 'r', encoding='utf-8') as f:
                translations = json.load(f)
            
            translations_count = 0
            for code_10, data in translations.items():
                code_10_str = str(code_10).strip()
                russian_name = data.get('russian_name', '').strip().upper()
                
                if not russian_name:
                    continue
                
                # Pad code to 10 digits on the RIGHT if needed (never remove leading zeros)
                code_10_padded = code_10_str.strip()
                if len(code_10_padded) >= 10:
                    code_10_padded = code_10_padded[:10]
                else:
                    code_10_padded = code_10_padded + '0' * (10 - len(code_10_padded))
                
                # Add translation for level 10 (only if not already in official mappings)
                if code_10_padded not in mappings['tnved10']:
                    mappings['tnved10'][code_10_padded] = {
                        'name': russian_name,
                        'translated': True
                    }
                    translations_count += 1
                
                # Also add translations for parent levels (2, 4, 6, 8) if they don't exist
                # Extract parent codes from padded 10-digit code (preserving leading zeros)
                for level in [2, 4, 6, 8]:
                    level_key = f'tnved{level}'
                    # Extract first N digits from padded code
                    code_level = code_10_padded[:level]
                    
                    # Only add if this level code doesn't exist in official mappings
                    if code_level not in mappings[level_key]:
                        # Use the russian_name from the 10-digit code as fallback
                        # Note: This is not ideal, but we don't have separate translations for parent levels
                        mappings[level_key][code_level] = {
                            'name': russian_name,  # Using the 10-digit name as fallback
                            'translated': True
                        }
            
            if translations_count > 0:
                logger.info(f"Loaded {translations_count} translated TNVED codes from {translations_file}")
        except Exception as e:
            logger.error(f"Failed to load TNVED translations from {translations_file}: {e}")
    else:
        logger.warning(f"TNVED translations file not found at {translations_file}")
    
    return mappings

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
        query_parts.append("    altQty")
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

    # Choose the supplementary quantity (KOL) and its unit code (EDIZM_CODE).
    # IMPORTANT: 
    # - qtyUnitCode is the PRIMARY unit CODE (usually weight in kg, code 8)
    # - qty is the PRIMARY quantity VALUES (weight values)
    # - altQtyUnitCode is the ALTERNATIVE/SUPPLEMENTARY unit CODE (what we need for EDIZM_CODE)
    # - altQty is the ALTERNATIVE/SUPPLEMENTARY quantity VALUES (what we need for KOL)
    # - qtyCode may exist as an additional source of unit CODE information
    #
    # Logic priority:
    # 1. Use altQtyUnitCode (CODE) and altQty (VALUES) if available - this is the supplementary unit we want
    # 2. If altQtyUnitCode is missing, check qtyCode (if it exists and is not kg/code 8) - use qtyCode for CODE and qty for VALUES
    # 3. Fallback to qtyUnitCode (CODE) and qty (VALUES) only if no alternative is available (but this is weight, not ideal)
    
    # Initialize EDIZM_CODE and KOL columns
    comtrade_df['EDIZM_CODE'] = None
    comtrade_df['KOL'] = None
    
    # Check if qtyCode column exists (may contain unit CODE information)
    has_qty_code = 'qtyCode' in comtrade_df.columns
    
    if 'altQtyUnitCode' in comtrade_df.columns and 'altQty' in comtrade_df.columns:
        # Priority 1: Use altQtyUnitCode (CODE) and altQty (VALUES) - this is the supplementary unit
        # altQtyUnitCode is the CODE of the supplementary unit
        # altQty is the VALUES in that supplementary unit
        # They can be independent - if we have the code, use it even if values are missing
        has_alt_code = comtrade_df['altQtyUnitCode'].notna()
        has_alt_values = comtrade_df['altQty'].notna()
        
        # Use altQtyUnitCode for EDIZM_CODE (unit code) if available
        comtrade_df.loc[has_alt_code, 'EDIZM_CODE'] = comtrade_df.loc[has_alt_code, 'altQtyUnitCode']
        # Use altQty for KOL (values) if available
        comtrade_df.loc[has_alt_values, 'KOL'] = comtrade_df.loc[has_alt_values, 'altQty']
        
        # Priority 2: If altQtyUnitCode is missing, check qtyCode (if available and not kg)
        # Use qtyCode for EDIZM_CODE (unit code) and qty for KOL (values)
        missing_alt_code = ~has_alt_code
        if has_qty_code:
            # Use qtyCode (CODE) if it exists, is not NULL, and is not 8 (kg)
            # Only use if EDIZM_CODE is still missing (altQtyUnitCode was not available)
            # Use qty (VALUES) for KOL if KOL is still missing
            qty_code_available = (
                missing_alt_code & 
                comtrade_df['qtyCode'].notna() & 
                (comtrade_df['qtyCode'] != 8)
            )
            comtrade_df.loc[qty_code_available, 'EDIZM_CODE'] = comtrade_df.loc[qty_code_available, 'qtyCode']
            
            # Use qty for KOL if KOL is still missing
            missing_kol = comtrade_df['KOL'].isna() & comtrade_df['qty'].notna()
            comtrade_df.loc[missing_kol, 'KOL'] = comtrade_df.loc[missing_kol, 'qty']
            missing_alt_code = missing_alt_code & ~qty_code_available
        
        # Priority 3: Fallback to qtyUnitCode (CODE) and qty (VALUES) - but this is weight, not ideal
        # Only use if EDIZM_CODE is still missing
        missing_edizm_code = comtrade_df['EDIZM_CODE'].isna()
        fallback_mask = missing_edizm_code & comtrade_df['qtyUnitCode'].notna()
        comtrade_df.loc[fallback_mask, 'EDIZM_CODE'] = comtrade_df.loc[fallback_mask, 'qtyUnitCode']
        
        # Use qty for KOL if KOL is still missing
        missing_kol = comtrade_df['KOL'].isna() & comtrade_df['qty'].notna()
        comtrade_df.loc[missing_kol, 'KOL'] = comtrade_df.loc[missing_kol, 'qty']
        
        # Log statistics
        logger.info(f"Unit mapping statistics:")
        logger.info(f"  - Using altQtyUnitCode (CODE): {has_alt_code.sum()}")
        logger.info(f"  - Using altQty (VALUES): {has_alt_values.sum()}")
        if has_qty_code:
            qty_code_used = missing_alt_code & comtrade_df['qtyCode'].notna() & (comtrade_df['qtyCode'] != 8)
            logger.info(f"  - Using qtyCode (CODE) where altQtyUnitCode missing: {qty_code_used.sum()}")
        logger.info(f"  - Using qtyUnitCode (CODE) as fallback: {fallback_mask.sum()}")
        logger.info(f"  - Using qty (VALUES) where altQty missing: {missing_kol.sum()}")
        logger.info(f"  - Missing EDIZM_CODE: {comtrade_df['EDIZM_CODE'].isna().sum()}")
        logger.info(f"  - Missing KOL: {comtrade_df['KOL'].isna().sum()}")
        
        # Diagnostic: Check for code 37 (Becquerels)
        code_37_mask = comtrade_df['altQtyUnitCode'] == 37
        if code_37_mask.any():
            logger.info(f"  - Found {code_37_mask.sum()} rows with altQtyUnitCode = 37 (Becquerels)")
            logger.info(f"  - Of these, {comtrade_df.loc[code_37_mask, 'EDIZM_CODE'].notna().sum()} have EDIZM_CODE set")
    else:
        logger.warning("altQtyUnitCode or altQty not found in Comtrade data.")
        # If altQtyUnitCode is not available, try qtyCode first
        if has_qty_code:
            # Use qtyCode (CODE) if it exists and is not 8 (kg)
            # Use qty (VALUES) for KOL
            qty_code_available = (
                comtrade_df['qtyCode'].notna() & 
                (comtrade_df['qtyCode'] != 8) &
                comtrade_df['qty'].notna()
            )
            comtrade_df.loc[qty_code_available, 'EDIZM_CODE'] = comtrade_df.loc[qty_code_available, 'qtyCode']
            comtrade_df.loc[qty_code_available, 'KOL'] = comtrade_df.loc[qty_code_available, 'qty']
            # For remaining rows, use qtyUnitCode (CODE) and qty (VALUES) as fallback
            remaining_mask = ~qty_code_available & comtrade_df['qty'].notna()
            comtrade_df.loc[remaining_mask, 'EDIZM_CODE'] = comtrade_df.loc[remaining_mask, 'qtyUnitCode']
            comtrade_df.loc[remaining_mask, 'KOL'] = comtrade_df.loc[remaining_mask, 'qty']
            logger.info(f"  - Using qtyCode (CODE) + qty (VALUES): {qty_code_available.sum()}")
            logger.info(f"  - Using qtyUnitCode (CODE) + qty (VALUES) as fallback: {remaining_mask.sum()}")
        else:
            # Last resort: use qtyUnitCode (CODE) and qty (VALUES) - but this is weight
            comtrade_df['EDIZM_CODE'] = comtrade_df['qtyUnitCode']
            comtrade_df['KOL'] = comtrade_df['qty']
            logger.warning("  - Using qtyUnitCode (CODE) + qty (VALUES) as last resort (this is weight, not ideal)")

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

    # Generate derived TNVED columns
    # Pad TNVED to 10 digits on the RIGHT if needed (never remove leading zeros)
    comtrade_df['TNVED'] = comtrade_df['TNVED'].astype(str).str.strip()
    
    def pad_right(code):
        if len(code) >= 10:
            return code[:10]
        return code + '0' * (10 - len(code))
    comtrade_df['TNVED'] = comtrade_df['TNVED'].apply(pad_right)
    
    # Generate derived columns from TNVED (preserving leading zeros)
    comtrade_df['TNVED2'] = comtrade_df['TNVED'].str.slice(0, 2)
    comtrade_df['TNVED4'] = comtrade_df['TNVED'].str.slice(0, 4)
    comtrade_df['TNVED6'] = comtrade_df['TNVED'].str.slice(0, 6)
    comtrade_df['TNVED8'] = comtrade_df['TNVED'].str.slice(0, 8)
    
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

    # Separate fizob files from regular data files
    fizob_files = [f for f in parquet_files if f.name.startswith('fizob')]
    regular_files = [f for f in parquet_files if not f.name.startswith('fizob')]
    
    logger.info(f"Found {len(fizob_files)} fizob files: {[f.name for f in fizob_files]}")
    logger.info(f"Found {len(regular_files)} regular data files: {[f.name for f in regular_files]}")

    # Load and validate national datasets (excluding fizob files)
    national_datasets = {}
    if regular_files:
        for file_path in regular_files:
            country_code = file_path.stem.replace('_full', '').upper()
            if country_code in excluded_countries_upper:
                logger.info(f"Skipping {file_path.name} as per --exclude-countries argument.")
                continue

            df = load_and_validate_file(file_path, start_year=args.start_year)
            if df is not None:
                df_processed = generate_derived_columns(df)
                # Ensure STRANA is uppercase for consistency
                if 'STRANA' in df_processed.columns:
                    df_processed['STRANA'] = df_processed['STRANA'].str.upper()
                national_datasets[country_code.lower()] = df_processed
    else:
        logger.warning("No regular national parquet files found in data_processed directory.")
    
    # Load fizob files and transform to unified fizob_index format
    fizob_index_rows = []
    if fizob_files:
        logger.info("Loading fizob files for unified fizob_index table...")
        for file_path in fizob_files:
            file_stem = file_path.stem
            df = load_and_validate_file(file_path, start_year=args.start_year)
            if df is not None:
                df_processed = generate_derived_columns(df)
                if 'STRANA' in df_processed.columns:
                    df_processed['STRANA'] = df_processed['STRANA'].str.upper()
                df_unified = transform_fizob_to_unified(df_processed, file_stem)
                if not df_unified.empty:
                    fizob_index_rows.append(df_unified)
                    logger.info(f"Loaded {file_stem}: {len(df_unified)} rows -> fizob_index")

    all_dataframes = []
    national_countries_iso = []

    # Process national data
    for source_name, df in national_datasets.items():
        df['SOURCE'] = 'national'
        all_dataframes.append(df)
        if 'STRANA' in df.columns and not df.empty:
            # Get all unique country codes from this dataset and ensure uppercase
            unique_countries = df['STRANA'].dropna().unique()
            for country in unique_countries:
                country_upper = country.upper()
                if country_upper not in national_countries_iso:
                    national_countries_iso.append(country_upper)
    
    # Process Comtrade data if flag is set
    if args.include_comtrade:
        if not comtrade_db_path.exists():
            logger.error(f"Comtrade database not found at {comtrade_db_path}. Cannot include Comtrade data.")
        else:
            # Always exclude national data countries from Comtrade pull
            # And also add any user-specified exclusions
            countries_to_exclude_from_comtrade = list(set(national_countries_iso + excluded_countries_upper))
            logger.info(f"Excluding countries from Comtrade data to avoid duplicates: {countries_to_exclude_from_comtrade}")

            comtrade_df = load_and_transform_comtrade(
                comtrade_db_path, 
                project_root, 
                exclude_countries=countries_to_exclude_from_comtrade,
                start_year=args.start_year
            )
            if not comtrade_df.empty:
                # Double-check: filter out any national countries that might have slipped through
                initial_comtrade_rows = len(comtrade_df)
                indices_to_drop = comtrade_df[comtrade_df['STRANA'].isin(national_countries_iso)].index
                comtrade_df.drop(indices_to_drop, inplace=True)
                filtered_rows = initial_comtrade_rows - len(comtrade_df)
                if filtered_rows > 0:
                    logger.info(f"Filtered {filtered_rows:,} duplicate rows from Comtrade data that matched national countries.")
                
                comtrade_df['SOURCE'] = 'comtrade'
                all_dataframes.append(comtrade_df)

    if not all_dataframes:
        logger.error("No data available to merge.")
        return

    # Merge all datasets
    merged_df = pd.concat(all_dataframes, ignore_index=True)
    
    # Apply country exclusions to the final merged dataset
    if excluded_countries_upper:
        initial_rows = len(merged_df)
        indices_to_drop = merged_df[merged_df['STRANA'].isin(excluded_countries_upper)].index
        merged_df.drop(indices_to_drop, inplace=True)
        excluded_rows = initial_rows - len(merged_df)
        if excluded_rows > 0:
            logger.info(f"Excluded {excluded_rows:,} rows for countries: {excluded_countries_upper}")
    
    merged_df = merged_df.sort_values(['PERIOD', 'STRANA', 'TNVED'])
    
    # Remove rows where NAPR is NULL
    initial_rows = len(merged_df)
    merged_df.dropna(subset=['NAPR'], inplace=True)
    null_napr_rows = initial_rows - len(merged_df)
    if null_napr_rows > 0:
        logger.info(f"Removed {null_napr_rows:,} rows with NULL NAPR values")

    # Standardize EDIZM column
    logger.info("Standardizing EDIZM column...")
    common_edizm_map = load_common_edizm_mapping(project_root)
    if common_edizm_map:
        # Normalize original EDIZM values before mapping (astype(str) is crucial)
        merged_df['EDIZM_upper'] = merged_df['EDIZM'].astype(str).str.upper().str.strip()
        
        # Additional normalization: replace common variants
        # Replace superscript ³ with regular 3
        merged_df['EDIZM_upper'] = merged_df['EDIZM_upper'].str.replace('³', '3', regex=False)
        # Replace superscript ² with regular 2
        merged_df['EDIZM_upper'] = merged_df['EDIZM_upper'].str.replace('²', '2', regex=False)
        # Normalize "/" to space for some variants (e.g., "KG/NET EDA" -> "KG NET EDA")
        # But keep "/" for cases like "CE/EL" which have specific mappings
        merged_df['EDIZM_upper'] = merged_df['EDIZM_upper'].str.replace(r'KG/NET', 'KG NET', regex=False)
        # Normalize parentheses: remove spaces around parentheses (e.g., "U (JEU/PACK)" -> "U (JEU/PACK)" but normalize spacing)
        merged_df['EDIZM_upper'] = merged_df['EDIZM_upper'].str.replace(r'\s*\(\s*', ' (', regex=True)
        merged_df['EDIZM_upper'] = merged_df['EDIZM_upper'].str.replace(r'\s*\)\s*', ')', regex=True)
        # Normalize multiple spaces to single space
        merged_df['EDIZM_upper'] = merged_df['EDIZM_upper'].str.replace(r'\s+', ' ', regex=True)
        # Remove trailing/leading spaces again after normalization
        merged_df['EDIZM_upper'] = merged_df['EDIZM_upper'].str.strip()
        # Handle special cases: '?' and 'N/A' should remain as is (don't map them)
        # They will be left as unmapped, which is fine
        
        # Map to common representation
        mapped_values = merged_df['EDIZM_upper'].map(common_edizm_map)
        
        # Update EDIZM and EDIZM_ISO
        merged_df['EDIZM'] = mapped_values.map(lambda x: x['NAME'] if pd.notna(x) else None)
        merged_df['EDIZM_ISO'] = mapped_values.map(lambda x: x['KOD'] if pd.notna(x) else None)
        
        # Handle unmapped values
        unmapped_mask = merged_df['EDIZM'].isnull()
        if unmapped_mask.sum() > 0:
            logger.warning(f"{unmapped_mask.sum()} EDIZM values could not be mapped to a common standard.")
            unmapped_sample = merged_df[unmapped_mask]['EDIZM_upper'].unique()
            logger.warning(f"Unmapped EDIZM sample: {unmapped_sample[:10]}")
        
        # Diagnostic: Check if "Bq" was mapped
        bq_mask = merged_df['EDIZM_upper'] == 'BQ'
        if bq_mask.any():
            bq_count = bq_mask.sum()
            bq_mapped = merged_df.loc[bq_mask, 'EDIZM'].notna().sum()
            logger.info(f"  - Found {bq_count} rows with EDIZM_upper = 'BQ'")
            logger.info(f"  - Of these, {bq_mapped} were successfully mapped to canonical name")
            if bq_mapped < bq_count:
                bq_unmapped = merged_df.loc[bq_mask & merged_df['EDIZM'].isna(), 'EDIZM_upper'].unique()
                logger.warning(f"  - Unmapped 'BQ' values (sample): {bq_unmapped[:5]}")
                # Check if 'БЕККЕРЕЛЬ' exists in mapping and if 'BQ' is in mapping
                logger.info(f"  - Checking if 'БЕККЕРЕЛЬ' exists in mapping: {'БЕККЕРЕЛЬ' in common_edizm_map}")
                logger.info(f"  - Checking if 'BQ' exists in mapping: {'BQ' in common_edizm_map}")
                if 'BQ' in common_edizm_map:
                    bq_mapping_value = common_edizm_map['BQ']
                    logger.info(f"  - 'BQ' maps to: {bq_mapping_value}")
            else:
                # All BQ values were mapped - show what they mapped to
                bq_mapped_values = merged_df.loc[bq_mask, 'EDIZM'].unique()
                logger.info(f"  - All 'BQ' values mapped to: {bq_mapped_values}")
            
        merged_df.drop(columns=['EDIZM_upper'], inplace=True)
    else:
        logger.error("Could not standardize EDIZM values due to mapping load failure.")

    # Nullify KOL where EDIZM is БЕККЕРЕЛЬ (values are too large and cause outliers)
    logger.info("Checking for БЕККЕРЕЛЬ units to nullify KOL values...")
    if 'EDIZM' in merged_df.columns:
        bekkerele_mask = merged_df['EDIZM'] == 'БЕККЕРЕЛЬ'
        num_bekkerele_rows = bekkerele_mask.sum()
        
        if num_bekkerele_rows > 0:
            logger.info(f"Found {num_bekkerele_rows:,} rows where EDIZM is БЕККЕРЕЛЬ. "
                       f"Setting KOL to NULL for these rows (values are too large).")
            merged_df.loc[bekkerele_mask, 'KOL'] = None
    else:
        logger.warning("Cannot perform БЕККЕРЕЛЬ check: EDIZM column not found.")

    # Nullify KOL where EDIZM is KG to avoid duplication with NETTO
    logger.info("Checking for supplementary units in KG to avoid duplication with NETTO...")
    kg_iso_code = '166'  # ISO code for Kilogram
    if 'EDIZM_ISO' in merged_df.columns:
        # Use .loc to avoid SettingWithCopyWarning
        kg_rows_mask = merged_df['EDIZM_ISO'] == kg_iso_code
        num_kg_rows = kg_rows_mask.sum()

        if num_kg_rows > 0:
            logger.info(f"Found {num_kg_rows:,} rows where the supplementary unit is KG. "
                        f"Setting KOL, EDIZM, and EDIZM_ISO to NULL for these rows.")
            merged_df.loc[kg_rows_mask, 'KOL'] = None
            merged_df.loc[kg_rows_mask, 'EDIZM'] = None
            merged_df.loc[kg_rows_mask, 'EDIZM_ISO'] = None
    else:
        logger.warning("Cannot perform KG duplication check: EDIZM_ISO column not found.")

    # Handle Tonnes: convert to KG if NETTO is missing, otherwise nullify to avoid duplication
    logger.info("Checking for supplementary units in Tonnes to convert or remove...")
    tonne_iso_code = '168'
    if 'EDIZM_ISO' in merged_df.columns:
        tonne_mask = (merged_df['EDIZM_ISO'] == tonne_iso_code) & merged_df['KOL'].notna()
        num_tonne_rows = tonne_mask.sum()

        if num_tonne_rows > 0:
            logger.info(f"Found {num_tonne_rows:,} rows with supplementary unit in Tonnes.")
            
            # Case 1: NETTO is missing or zero, so we can backfill it from KOL
            netto_missing_mask = tonne_mask & ((merged_df['NETTO'].isnull()) | (merged_df['NETTO'] == 0))
            num_to_convert = netto_missing_mask.sum()
            if num_to_convert > 0:
                logger.info(f"  - Converting {num_to_convert:,} Tonne values to KG and filling NETTO.")
                # Convert Tonnes in KOL to KG and assign to NETTO
                merged_df.loc[netto_missing_mask, 'NETTO'] = merged_df.loc[netto_missing_mask, 'KOL'] * 1000
                # Nullify the supplementary unit columns as the value is now in NETTO
                merged_df.loc[netto_missing_mask, 'KOL'] = None
                merged_df.loc[netto_missing_mask, 'EDIZM'] = None
                merged_df.loc[netto_missing_mask, 'EDIZM_ISO'] = None

            # Case 2: NETTO already has a value, so KOL is redundant
            # Re-calculate the mask to only affect rows not already handled above
            tonne_mask = (merged_df['EDIZM_ISO'] == tonne_iso_code) & merged_df['KOL'].notna()
            netto_present_mask = tonne_mask & merged_df['NETTO'].notna() & (merged_df['NETTO'] != 0)
            num_to_remove = netto_present_mask.sum()
            if num_to_remove > 0:
                logger.info(f"  - Removing {num_to_remove:,} redundant Tonne values as NETTO is already populated.")
                merged_df.loc[netto_present_mask, 'KOL'] = None
                merged_df.loc[netto_present_mask, 'EDIZM'] = None
                merged_df.loc[netto_present_mask, 'EDIZM_ISO'] = None
    else:
        logger.warning("Cannot perform Tonne duplication check: EDIZM_ISO column not found.")

    # Note: Country names and TNVED names are now stored in separate reference tables
    # and can be joined via the unified_trade_data_enriched view or directly in queries
    logger.info("Reference tables (country names, TNVED names) will be created as separate tables in the database.")

    # Save to DuckDB
    save_to_duckdb(merged_df, output_db_path)
    
    # Save unified fizob_index table
    if fizob_index_rows:
        logger.info("Saving unified fizob_index table...")
        fizob_index_df = pd.concat(fizob_index_rows, ignore_index=True)
        conn = duckdb.connect(str(output_db_path))
        try:
            chunk_size = 100000
            first_chunk = fizob_index_df.iloc[:chunk_size]
            conn.register('fizob_chunk_df', first_chunk)
            conn.execute("""
                CREATE OR REPLACE TABLE fizob_index AS 
                SELECT STRANA, NAPR, CAST(PERIOD AS DATE) AS PERIOD, tn_level, tn_code, fizob, fizob_bp
                FROM fizob_chunk_df
            """)
            conn.unregister('fizob_chunk_df')
            logger.info(f"  ... created fizob_index and inserted first {len(first_chunk):,} rows")
            
            for i in range(chunk_size, len(fizob_index_df), chunk_size):
                chunk = fizob_index_df.iloc[i:i + chunk_size]
                conn.register('fizob_chunk_df', chunk)
                conn.execute("""
                    INSERT INTO fizob_index
                    SELECT STRANA, NAPR, CAST(PERIOD AS DATE) AS PERIOD, tn_level, tn_code, fizob, fizob_bp
                    FROM fizob_chunk_df
                """)
                conn.unregister('fizob_chunk_df')
                logger.info(f"  ... inserted {i + len(chunk):,} / {len(fizob_index_df):,} rows")
            
            result = conn.execute("SELECT COUNT(*) FROM fizob_index").fetchone()
            logger.info(f"  ... saved {result[0]:,} rows to fizob_index")
            
            conn.execute("""
                CREATE OR REPLACE VIEW fizob_index_v AS
                SELECT *,
                       CASE WHEN fizob_bp = 0 THEN NULL ELSE fizob / fizob_bp END AS idx
                FROM fizob_index
            """)
            logger.info("  ... created view fizob_index_v with computed idx column")
            conn.close()
        except Exception as e:
            conn.close()
            logger.error(f"Failed to save fizob_index: {e}")
            raise
    
    # Save reference tables and create convenience view
    conn = duckdb.connect(str(output_db_path))
    try:
        save_reference_tables(conn, project_root)
        conn.close()
    except Exception as e:
        conn.close()
        logger.error(f"Failed to create reference tables: {e}")
        raise
    
    logger.info("Data merge completed. To process outliers, run: python src/outlier_detection.py")

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

if __name__ == "__main__":
    main()