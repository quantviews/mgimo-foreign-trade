import pandas as pd
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def extract_yearly_metadata():
    """
    Extracts commodity metadata from yearly China trade data CSVs
    into year-specific JSON files.
    """
    # Define paths relative to script location
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    
    # --- Setup for EDIZM_ISO mapping ---
    # Define paths
    edizm_file = project_root / 'metadata' / 'edizm.csv'
    
    # Read EDIZM mapping file
    try:
        edizm_df = pd.read_csv(edizm_file, dtype=str, na_filter=False)
        # Standardize column names to uppercase
        edizm_df.columns = edizm_df.columns.str.upper()
        # Create a mapping from Russian name (uppercase) to ISO code
        edizm_map = edizm_df.set_index('NAME')['KOD'].to_dict()
        logger.info(f"Loaded EDIZM mapping with {len(edizm_map)} entries")
    except FileNotFoundError:
        logger.warning(f"EDIZM mapping file not found at {edizm_file}. Cannot add EDIZM_ISO codes.")
        edizm_map = {}
    except Exception as e:
        logger.error(f"Failed to load EDIZM mapping file: {e}")
        edizm_map = {}

    # Hardcoded map from English units in data to Russian units in edizm.csv
    eng_to_rus_map = {
        'Kilogram': 'КИЛОГРАММ',
        'Number of item': 'ШТУКА',
        'Gram': 'ГРАММ',
        'Litre': 'ЛИТР',
        'Metre': 'МЕТР',
        'Square metre': 'КВАДРАТНЫЙ МЕТР',
        'Cubic metre': 'КУБИЧЕСКИЙ МЕТР',
        'Pair': 'ПАРА',
        'Thousand items': 'ТЫСЯЧА ШТУК',
        'Carat': 'МЕТРИЧЕСКИЙ КАРАТ(1КАРАТ=2*10(-4)КГ'
    }
    # --- End of setup ---

    # Define paths
    input_dir = project_root / 'data_raw' / 'china' / 'YEAR'
    
    if not input_dir.exists():
        logger.error(f"Input directory does not exist: {input_dir}")
        return
    
    # Get all csv files from the input directory
    csv_files = list(input_dir.glob('*.csv'))

    if not csv_files:
        logger.warning(f"No CSV files found in {input_dir}")
        return
    
    logger.info(f"Found {len(csv_files)} CSV files to process")

    for csv_file in csv_files:
        # Extract year from filename
        try:
            year = int(csv_file.stem)
        except ValueError:
            logger.warning(f"Could not parse year from filename: {csv_file.name}. Skipping.")
            continue
        
        # Read csv file with gb18030 encoding and python engine, warning on bad lines
        try:
            df = pd.read_csv(csv_file, encoding='gb18030', engine='python', on_bad_lines='warn')
            
            if df.empty:
                logger.warning(f"File {csv_file.name} is empty. Skipping.")
                continue
                
            logger.info(f"Processing {csv_file.name}: {len(df)} rows")
        except Exception as e:
            logger.error(f"Failed to read {csv_file.name}: {e}. Skipping.")
            continue
        
        # --- Metadata Extraction ---
        # Define metadata path and create directory
        metadata_dir = project_root / 'metadata' / 'china'
        metadata_dir.mkdir(parents=True, exist_ok=True)
        metadata_file = metadata_dir / f"{year}-codes.json"

        # Define columns for metadata extraction and new names based on data_model.md
        # Using logical names for columns not explicitly in data_model.md
        metadata_cols = {
            "Commodity code": "TNVED",
            "Commodity": "COMMODITY_NAME",
            "Unit": "NETTO_UNIT",
            "Supplementary Unit": "EDIZM"
        }
        
        # Extract, deduplicate, rename, and save metadata to JSON
        if all(col in df.columns for col in metadata_cols.keys()):
            metadata_df = df[list(metadata_cols.keys())].drop_duplicates().copy()
            metadata_df.rename(columns=metadata_cols, inplace=True)
            
            # Ensure TNVED is string and pad to 10 characters for consistency
            if 'TNVED' in metadata_df.columns:
                metadata_df['TNVED'] = metadata_df['TNVED'].astype(str).str.zfill(10)

            # Add EDIZM_ISO column by mapping through English -> Russian -> ISO code
            if edizm_map:
                # Map English -> Russian -> ISO code
                metadata_df['EDIZM_ISO'] = metadata_df['EDIZM'].map(eng_to_rus_map)
                metadata_df['EDIZM_ISO'] = metadata_df['EDIZM_ISO'].map(edizm_map)
                
                # Fill missing values with None (will be null in JSON)
                metadata_df['EDIZM_ISO'] = metadata_df['EDIZM_ISO'].where(metadata_df['EDIZM_ISO'].notna(), None)
                
                # Log unmapped values
                unmapped = metadata_df[metadata_df['EDIZM_ISO'].isna()]['EDIZM'].unique()
                if len(unmapped) > 0:
                    logger.warning(f"Found {len(unmapped)} unmapped EDIZM values in {year}: {list(unmapped[:5])}")
            else:
                metadata_df['EDIZM_ISO'] = None
            
            # Save to JSON
            try:
                metadata_df.to_json(
                    metadata_file, 
                    orient='records', 
                    indent=4, 
                    force_ascii=False
                )
                logger.info(f"Metadata for {year} saved to {metadata_file} ({len(metadata_df)} records)")
            except Exception as e:
                logger.error(f"Failed to save metadata for {year}: {e}")
        else:
            missing_cols = set(metadata_cols.keys()) - set(df.columns)
            logger.warning(f"Skipping metadata extraction for {csv_file.name}: missing columns {missing_cols}")
        # --- End Metadata Extraction ---

if __name__ == "__main__":
    extract_yearly_metadata()
