import pandas as pd
from pathlib import Path
import os
import logging
from typing import Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_edizm_russian_mapping(edizm_file: Path) -> Dict[str, str]:
    """
    Loads a mapping from EDIZM ISO codes to their Russian names from edizm.csv.

    Args:
        edizm_file: Path to the metadata/edizm.csv file.

    Returns:
        A dictionary mapping ISO codes (str) to Russian names (str).
    """
    if not edizm_file.exists():
        logger.warning(f"EDIZM mapping file not found: {edizm_file}")
        return {}

    try:
        df = pd.read_csv(edizm_file, dtype={'KOD': str})
        # Clean the KOD column by removing quotes and stripping whitespace
        df['KOD'] = df['KOD'].str.replace('"', '').str.strip()
        mapping = pd.Series(df.NAME.values, index=df.KOD).to_dict()
        logger.info(f"Loaded Russian EDIZM mapping with {len(mapping)} entries.")
        return mapping
    except Exception as e:
        logger.error(f"Failed to load or process EDIZM mapping file {edizm_file}: {e}")
        return {}


def load_china_codes_mapping(codes_dir: Path) -> dict:
    """
    Load china codes mapping from CSV files to get EDIZM and EDIZM_ISO mapping.
    
    Args:
        codes_dir: Path to metadata/china-codes directory
        
    Returns:
        Dictionary mapping TNVED codes to units info
    """
    logger.info("Loading China codes mapping...")
    
    # Find all china codes CSV files
    csv_files = list(codes_dir.glob("*-china.csv"))
    if not csv_files:
        logger.warning("No china codes CSV files found")
        return {}
    
    # Create English to ISO mapping for UNIT_2_DESCRIPTION
    english_to_iso_mapping = {
        # Common units from china-codes CSV
        "Number of item": "796",          # ШТУКА
        "Piece": "796",                  # ШТУКА  
        "Pair": "715",                   # ПАРА
        "Square Metre": "055",           # КВАДРАТНЫЙ МЕТР
        "Metre": "006",                  # МЕТР
        "Kilogram": "166",               # КИЛОГРАММ
        "Litre": "112",                  # ЛИТР
        "Cubic Metre": "113",            # КУБИЧЕСКИЙ МЕТР
        "Carat": "162",                  # МЕТРИЧЕСКИЙ КАРАТ
        "Gram": "163",                   # ГРАММ
        "In Hundreds": "797",            # СТО ШТУК
        "In Thousands": "798",           # ТЫСЯЧА ШТУК
        "Million Bq": "305",             # КЮРИ
    }
    
    # Process all CSV files and create mapping
    codes_mapping = {}
    
    for csv_file in csv_files:
        try:
            year = csv_file.stem.split('-')[0]
            logger.info(f"Processing {csv_file.name} (year: {year})")
            
            # Try different encodings and parsing options for CSV files
            encodings_to_try = ['utf-8', 'utf-8-sig', 'cp1252', 'cp1251', 'latin1', 'iso-8859-1']
            df = None
            successful_encoding = None
            
            for encoding in encodings_to_try:
                try:
                    df = pd.read_csv(csv_file, encoding=encoding, on_bad_lines='skip', quoting=1)
                    # Check if we got any data
                    if len(df) > 0 and 'CODES' in df.columns:
                        successful_encoding = encoding
                        logger.debug(f"Successfully loaded {csv_file.name} with {encoding} encoding ({len(df)} rows)")
                        break
                    df = None
                except (UnicodeDecodeError, pd.errors.ParserError, FileNotFoundError) as e:
                    logger.debug(f"Failed to load {csv_file.name} with {encoding}: {str(e)[:50]}...")
                    continue
            
            if df is None:
                logger.error(f"Could not decode {csv_file.name} with any encoding")
                continue
            
            # Create mapping for this year
            for _, row in df.iterrows():
                tnved_code = str(row['CODES']).zfill(8)  # Ensure 8 digits
                unit_description = row['UNIT_2_DESCRIPTION']
                
                codes_mapping[tnved_code] = {
                    'EDIZM': unit_description,
                    'EDIZM_ISO': english_to_iso_mapping.get(unit_description, '?')
                }
                
        except Exception as e:
            logger.error(f"Failed to process {csv_file.name}: {e}")
    
    # Report mapping coverage
    mapped_units = set()
    unmapped_units = set()
    
    for mapping in codes_mapping.values():
        if mapping['EDIZM_ISO'] != '?':
            mapped_units.add(mapping['EDIZM'])
        else:
            unmapped_units.add(mapping['EDIZM'])
    
    logger.info(f"Loaded codes mapping with {len(codes_mapping)} TNVED codes")
    logger.info(f"Successfully mapped units: {sorted(mapped_units)}")
    if unmapped_units:
        logger.warning(f"Unmapped units found: {sorted(unmapped_units)}")
        logger.warning("Consider adding these to english_to_iso_mapping")
    
    return codes_mapping

def process_and_merge_china_data(
    raw_data_dir: Path, 
    output_file: Path, 
    codes_dir: Path = None, 
    edizm_file: Path = None
):
    """
    Scans for raw china data files, processes them according to the logic
    from ProcessData.ipynb, merges them into a single DataFrame, and saves
    as a Parquet file.
    """
    
    # 0. Load china codes and EDIZM mappings
    codes_mapping = {}
    if codes_dir and codes_dir.exists():
        codes_mapping = load_china_codes_mapping(codes_dir)
        
    edizm_rus_mapping = {}
    if edizm_file and edizm_file.exists():
        edizm_rus_mapping = load_edizm_russian_mapping(edizm_file)
    
    # 1. Find all raw data files generated by the collector
    logger.info("Searching for raw data files...")
    import_dir = raw_data_dir / 'IMPORT'
    export_dir = raw_data_dir / 'EXPORT'
    
    all_files = list(import_dir.glob('data*.csv')) + list(export_dir.glob('data*.csv'))

    if not all_files:
        logger.error("No raw data files found to process in 'data_raw/china/'.")
        return

    logger.info(f"Found {len(all_files)} files to process.")
    
    # 2. Process each file and collect DataFrames
    processed_dfs = []
    for file_path in all_files:
        try:
            df = pd.read_csv(file_path)

            # Apply transformations based on refresh_ch()
            if 'PERIOD' in df.columns:
                df['PERIOD'] = df['PERIOD'] + '-01'
            
            if 'STRANA' in df.columns:
                df['STRANA'] = 'CN'
            
            # Convert STOIM from thousands USD to USD
            #if 'STOIM' in df.columns:
            #    df['STOIM'] = df['STOIM'] * 1000

            # Rename columns to match the old notebook's standard
            df = df.rename(columns={
                'Supplementary Quantity': 'KOL',
                # We no longer need 'Supplementary Unit' as EDIZM will be derived
            })

            # Ensure TNVED codes are zero-padded strings
            tnved_cols = {'TNVED': 8, 'TNVED2': 2, 'TNVED4': 4, 'TNVED6': 6}
            for col, length in tnved_cols.items():
                if col in df.columns:
                    df[col] = df[col].astype(str).str.zfill(length)
            
            # Initialize EDIZM and EDIZM_ISO columns
            df['EDIZM_ISO'] = '?'
            df['EDIZM'] = '?'
            
            # Add EDIZM and EDIZM_ISO from mappings
            if codes_mapping and 'TNVED' in df.columns:
                logger.info(f"Applying units mapping for {file_path.name}")
                
                # Get the ISO code from the TNVED mapping
                df['EDIZM_ISO'] = df['TNVED'].map(
                    lambda x: codes_mapping.get(str(x).zfill(8), {}).get('EDIZM_ISO', '?')
                )
                
                # Get the Russian name from the EDIZM ISO mapping
                if edizm_rus_mapping:
                    df['EDIZM'] = df['EDIZM_ISO'].map(edizm_rus_mapping).fillna('?')
                else:
                    # Fallback to English name if Russian mapping is unavailable
                    df['EDIZM'] = df['TNVED'].map(
                        lambda x: codes_mapping.get(str(x).zfill(8), {}).get('EDIZM', '?')
                    )

            # Reorder columns to a standard format
            standard_columns = [
                'NAPR', 'PERIOD', 'STRANA', 'TNVED', 'EDIZM', 'EDIZM_ISO', 'STOIM', 
                'NETTO', 'KOL', 'TNVED4', 'TNVED6', 'TNVED2'
            ]
            
            existing_cols = [col for col in standard_columns if col in df.columns]
            df = df.reindex(columns=existing_cols)
            
            processed_dfs.append(df)
            logger.info(f"  - Processed {file_path.name}")
        except Exception as e:
            logger.error(f"  - FAILED to process {file_path.name}: {e}")

    if not processed_dfs:
        logger.error("No data was successfully processed.")
        return

    # 3. Concatenate all DataFrames
    logger.info("Merging all processed files...")
    final_df = pd.concat(processed_dfs, ignore_index=True)

    # 4. Clean data before saving
    # The NETTO column can contain non-numeric characters (e.g., tabs)
    # Convert to string, strip whitespace, and then convert to numeric, coercing errors.
    if 'NETTO' in final_df.columns:
        logger.info("Cleaning 'NETTO' column...")
        final_df['NETTO'] = pd.to_numeric(
            final_df['NETTO'].astype(str).str.strip(), 
            errors='coerce'
        )

    if 'KOL' in final_df.columns:
        logger.info("Cleaning 'KOL' column...")
        final_df['KOL'] = pd.to_numeric(
            final_df['KOL'].astype(str).str.strip(),
            errors='coerce'
        )

    # 5. Check for missing months in the time series
    logger.info("Checking for gaps in the time series...")
    if 'PERIOD' in final_df.columns:
        # Ensure PERIOD is in datetime format for comparison
        final_df['PERIOD'] = pd.to_datetime(final_df['PERIOD'])
        
        # Get the unique months from the data
        actual_months = set(final_df['PERIOD'].dt.to_period('M'))
        
        # Create a complete date range from min to max date
        start_date = final_df['PERIOD'].min()
        end_date = final_df['PERIOD'].max()
        expected_months = set(pd.period_range(start=start_date, end=end_date, freq='M'))
        
        missing_months = sorted(list(expected_months - actual_months))
        
        if missing_months:
            logger.warning("Missing data for the following months:")
            for month in missing_months:
                logger.warning(f"  - {month}")
        else:
            logger.info("No missing months found in the time series.")

    # 6. Delete existing parquet file if it exists
    if output_file.exists():
        logger.info(f"Deleting existing file: {output_file}")
        os.remove(output_file)

    # 7. Save final DataFrame as Parquet
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Saving merged data to {output_file}...")
    try:
        final_df.to_parquet(output_file, index=False)
        logger.info("Processing complete.")
        
        # Summary statistics
        logger.info(f"Final dataset: {len(final_df)} rows")
        if 'EDIZM_ISO' in final_df.columns:
            non_null_edizm_iso = final_df['EDIZM_ISO'].notna().sum()
            mapped_iso = (final_df['EDIZM_ISO'] != '?').sum()
            logger.info(f"EDIZM_ISO mapping: {mapped_iso}/{len(final_df)} rows ({mapped_iso/len(final_df)*100:.1f}%) successfully mapped")
            logger.info(f"EDIZM_ISO filled: {non_null_edizm_iso}/{len(final_df)} rows ({non_null_edizm_iso/len(final_df)*100:.1f}%) have values")
        
    except ImportError:
        logger.error("'pyarrow' is required to save to Parquet format.")
        logger.error("Please install it using: pip install pyarrow")


def main():
    """
    Main function to run the China data processing script.
    """
    project_root = Path(__file__).resolve().parent.parent.parent
    raw_data_dir = project_root / 'data_raw' / 'china'
    codes_dir = project_root / 'metadata' / 'china-codes'
    edizm_file = project_root / 'metadata' / 'edizm.csv'
    output_file = project_root / 'data_processed' / 'cn_full.parquet'  # Using ISO 3166-1 alpha-2 code
    
    process_and_merge_china_data(raw_data_dir, output_file, codes_dir, edizm_file)

if __name__ == "__main__":
    main()
