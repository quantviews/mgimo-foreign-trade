import pandas as pd
import glob
from pathlib import Path

def extract_yearly_metadata():
    """
    Extracts commodity metadata from yearly China trade data CSVs
    into year-specific JSON files.
    """
    # --- Setup for EDIZM_ISO mapping ---
    # Define paths
    edizm_file = Path('metadata/edizm.csv')
    
    # Read EDIZM mapping file
    try:
        edizm_df = pd.read_csv(edizm_file)
        # Create a mapping from Russian name to ISO code
        edizm_map = edizm_df.set_index('NAME')['KOD'].to_dict()
    except FileNotFoundError:
        print(f"Warning: EDIZM mapping file not found at {edizm_file}. Cannot add EDIZM_ISO codes.")
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
    input_dir = Path('data_raw/china/YEAR/')
    
    # Get all csv files from the input directory
    csv_files = list(input_dir.glob('*.csv'))

    if not csv_files:
        print(f"No CSV files found in {input_dir}")
        return

    for csv_file in csv_files:
        # Extract year from filename
        try:
            year = int(csv_file.stem)
        except ValueError:
            print(f"Could not parse year from filename: {csv_file.name}. Skipping.")
            continue
        
        # Read csv file with gb18030 encoding and python engine, warning on bad lines
        df = pd.read_csv(csv_file, encoding='gb18030', engine='python', on_bad_lines='warn')
        
        # --- Metadata Extraction ---
        # Define metadata path and create directory
        metadata_dir = Path('metadata/china/')
        metadata_dir.mkdir(exist_ok=True)
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

            # Add EDIZM_ISO column by mapping through English -> Russian -> ISO code
            if edizm_map:
                metadata_df['EDIZM_ISO'] = metadata_df['EDIZM'].map(eng_to_rus_map).map(edizm_map)
                # Convert to nullable integer to handle missing values and prevent floats in JSON
                metadata_df['EDIZM_ISO'] = metadata_df['EDIZM_ISO'].astype('Int64')
            
            metadata_df.to_json(
                metadata_file, 
                orient='records', 
                indent=4, 
                force_ascii=False
            )
            print(f"Metadata for {year} saved to {metadata_file}")
        else:
            print(f"Skipping metadata extraction for {csv_file.name}: one or more required columns not found.")
        # --- End Metadata Extraction ---

if __name__ == "__main__":
    extract_yearly_metadata()
