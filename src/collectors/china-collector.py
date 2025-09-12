import os
import argparse
import pandas as pd
from pathlib import Path
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager


def get_download_path():
    """Returns the default downloads path for the current OS."""
    if os.name == 'nt':
        import winreg
        sub_key = r'SOFTWARE\Microsoft\Windows\CurrentVersion\Explorer\Shell Folders'
        downloads_guid = '{374DE290-123F-4565-9164-39C4925E467B}'
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, sub_key) as key:
            location = winreg.QueryValueEx(key, downloads_guid)[0]
        return Path(location)
    else:
        return Path.home() / "downloads"

def automate_download(year: str, month: str, flow: str, partner_code: str):
    """
    Opens the Chinese customs stats website, fills the form,
    and waits for the user to solve the CAPTCHA to download the data.
    """
    download_dir = get_download_path()
    source_file = download_dir / 'downloadData.csv'
    
    # Clean up old file if it exists
    if source_file.exists():
        os.remove(source_file)

    options = webdriver.ChromeOptions()
    # Stealth options to avoid bot detection
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    
    options.add_argument('--start-maximized')
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--allow-running-insecure-content')
    options.add_argument('--disable-extensions')
    # You might need to adjust preferences depending on your setup
    # prefs = {"download.default_directory": str(download_dir)}
    # options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
    # Evade detection
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    try:
        print("Opening browser to the Chinese customs statistics website...")
        driver.get("http://stats.customs.gov.cn/indexEn")
        
        print("\n" + "="*50)
        print("Please fill out the form, solve the CAPTCHA, and download the data file.")
        print("Once the 'downloadData.csv' file has finished downloading,")
        print("press Enter here to continue.")
        print("="*50)
        input()

        # Wait for download to be present, assuming user has completed it.
        print("Checking for downloaded file...")
        wait = WebDriverWait(driver, 300) # 5-minute timeout for manual process
        wait.until(lambda d: source_file.exists())
        
        print("Download complete.")
        return True

    except Exception as e:
        print(f"An error occurred during browser automation: {e}")
        return False
    finally:
        driver.quit()

def run_raw_data_checks(file_path: Path):
    """
    Runs a series of checks on the raw downloaded CSV file to ensure data integrity
    before any processing is done.
    """
    print("Running raw data quality checks...")
    checks_passed = True
    
    try:
        # Read only the first 5 rows for efficiency
        df_sample = pd.read_csv(file_path, encoding='ISO-8859-1', on_bad_lines='skip', nrows=5)

        # 1. Raw Column Check
        expected_raw_cols = {'Date of data', 'Trading partner code', 'Commodity code', 'Quantity'}
        
        # Check for one of the value columns
        has_value_col = 'Renminbi Yuan' in df_sample.columns or 'US dollar' in df_sample.columns
        
        missing_cols = expected_raw_cols - set(df_sample.columns)
        if missing_cols or not has_value_col:
            print(f"  [FAIL] Missing expected raw columns. Missing: {missing_cols}")
            if not has_value_col: print("  [FAIL] Missing a value column ('Renminbi Yuan' or 'US dollar').")
            checks_passed = False
        else:
            print("  [PASS] All expected raw columns are present.")

        # 2. Date Format Check
        if not (df_sample['Date of data'].astype(str).str.len() == 6).all():
            print("  [FAIL] 'Date of data' column does not have a length of 6.")
            checks_passed = False
        else:
            print("  [PASS] 'Date of data' column format is correct.")
        
        # 3. Numeric Value Check for Quantity
        # Ensure it's a string, remove commas, then check if it's all digits
        if not df_sample['Quantity'].astype(str).str.replace(',', '').str.isdigit().all():
            print("  [FAIL] 'Quantity' column contains non-numeric values.")
            checks_passed = False
        else:
            print("  [PASS] 'Quantity' column contains numeric values.")

    except Exception as e:
        print(f"  [FAIL] An error occurred during raw data checks: {e}")
        return False
        
    if not checks_passed:
        print("\nRaw data quality checks failed. Please check the downloaded file.")
        return False
        
    print("\nAll raw data quality checks passed successfully.")
    return True


def save_raw_data(year: str, month: str, flow: str, output_dir: Path):
    """
    Saves the manually downloaded 'downloadData.csv' file from Chinese customs as raw data.
    """
    downloads_path = get_download_path()
    source_file = downloads_path / 'downloadData.csv'
    
    if not source_file.exists():
        print(f"Error: '{source_file}' not found. Please download the file first.")
        return

    import_path = output_dir / 'IMPORT'
    export_path = output_dir / 'EXPORT'
    import_path.mkdir(parents=True, exist_ok=True)
    export_path.mkdir(parents=True, exist_ok=True)

    month_str = str(month).zfill(2)

    # Determine paths based on mirrored flow
    if flow == 'ИМ':
        raw_dest_path = export_path
    else:  # 'ЭК'
        raw_dest_path = import_path
        
    # Save the raw data
    raw_file_name = f"data{year}{month_str}.csv"
    raw_file_path = raw_dest_path / raw_file_name
    source_file.rename(raw_file_path)
    print(f"Saved raw data to '{raw_file_path}'")
    
    # Run Raw Data Quality Checks
    if not run_raw_data_checks(raw_file_path):
        # We might want to move the file to a 'quarantine' folder here in a real pipeline
        print("Raw data quality checks failed. File saved but not processed.")
        return False
    
    print("Raw data quality checks passed. File ready for processing.")
    return True


def main():
    """
    Main function to run the data collection and processing script.
    """
    # The project root is two levels up from the script location (src/collectors)
    project_root = Path(__file__).resolve().parent.parent.parent
    default_output = project_root / 'data_raw' / 'china'

    parser = argparse.ArgumentParser(description="Process Chinese customs data.")
    parser.add_argument("year", type=str, help="Year of the data (e.g., 2025)")
    parser.add_argument("month", type=str, help="Month of the data (e.g., 5 or 05)")
    parser.add_argument("flow", type=str, choices=['ИМ', 'ЭК'], help="Flow type: 'ИМ' for Import, 'ЭК' for Export")
    parser.add_argument("--partner", type=str, default="344", help="Partner code (e.g., 344 for Russia).")
    parser.add_argument("--output_dir", type=Path, default=default_output, help="Directory to save processed files.")
    
    args = parser.parse_args()
    
    month_padded = args.month.zfill(2)

    print(f"Starting automation for Year: {args.year}, Month: {month_padded}, Flow: {args.flow}, Partner: {args.partner}")
    
    # Step 1: Automate the download
    download_successful = automate_download(args.year, month_padded, args.flow, args.partner)

    # Step 2: Save the raw data
    if download_successful:
        print(f"Starting raw data saving...")
        save_raw_data(args.year, month_padded, args.flow, args.output_dir)

if __name__ == "__main__":
    # INSTRUCTIONS:
    # 1. Ensure you have Google Chrome installed.
    # 2. Run this script from the terminal. It will open a Chrome window.
    #    Example:
    #    python src/collectors/china-collector.py 2025 7 ИМ --partner 344
    # 3. When prompted, solve the CAPTCHA in the browser.
    # 4. After the data table appears, press Enter in your terminal.
    # 5. The script will download and save the raw data file.
    # 6. To process the raw data, run:
    #    python src/collectors/china_processor.py data_raw/china/EXPORT/data202507.csv 2025 07 ИМ
    main()