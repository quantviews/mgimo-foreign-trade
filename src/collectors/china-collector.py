import os
import argparse
import pandas as pd
from pathlib import Path
import time
# from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.chrome.service import Service as ChromeService
# from webdriver_manager.chrome import ChromeDriverManager
import undetected_chromedriver as uc


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

    options = uc.ChromeOptions()
    # Stealth options to avoid bot detection
    options.add_argument('--disable-blink-features=AutomationControlled')
    # options.add_experimental_option("excludeSwitches", ["enable-automation"])
    # options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36")
    
    options.add_argument('--start-maximized')
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--allow-running-insecure-content')
    options.add_argument('--disable-extensions')
    # You might need to adjust preferences depending on your setup
    # prefs = {"download.default_directory": str(download_dir)}
    # options.add_experimental_option("prefs", prefs)

    driver = uc.Chrome(options=options, version_main=141)
    # Evade detection
    # driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

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

def process_downloaded_data(year: str, month: str, flow: str, output_dir: Path):
    """
    Processes the manually downloaded 'downloadData.csv' file from Chinese customs.
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
    
    try:
        df = pd.read_csv(source_file, encoding='ISO-8859-1', on_bad_lines='skip')
        
        # Validate that all expected columns are present
        expected_cols = {
            'Date of data', 'Trading partner code', 'Trading partner', 
            'Commodity code', 'Commodity', 'Quantity', 'Unit', 
            'Supplementary Quantity', 'Supplementary Unit'
        }
        
        missing_cols = expected_cols - set(df.columns)
        if missing_cols:
            print(f"\n[ERROR] The downloaded file is missing required columns: {sorted(list(missing_cols))}")
            print("Please re-download, ensuring all columns are selected on the website.")
            os.remove(source_file)
            print(f"Removed incorrect source file: '{source_file}'")
            return
            
        # Validate the date in the file matches the arguments
        if 'Date of data' in df.columns and not df.empty:
            file_date = str(df['Date of data'].iloc[0])
            expected_date = f"{year}{month}"
            if file_date != expected_date:
                print(f"\n[ERROR] Date mismatch: The script was run for {expected_date}, but the file contains data for {file_date}.")
                print("Please re-download the correct data file.")
                os.remove(source_file)
                print(f"Removed incorrect source file: '{source_file}'")
                return
            
        # Rename columns
        rename_map = {
            'Commodity code': 'TNVED',
            # 'US dollar': 'STOIM', # This will be handled dynamically
            'Quantity': 'NETTO'
        }
        df = df.rename(columns=rename_map)

        # Dynamically handle currency column
        if 'US dollar' in df.columns:
            df = df.rename(columns={'US dollar': 'STOIM'})
        elif 'Renminbi Yuan' in df.columns:
            print("\n[ERROR] The downloaded file contains 'Renminbi Yuan' instead of 'US dollar'.")
            print("Please re-download the data, ensuring that 'US dollar' is selected as the currency.")
            os.remove(source_file)
            print(f"Removed incorrect source file: '{source_file}'")
            return
        else:
            print("Error: Could not find a currency column ('US dollar' or 'Renminbi Yuan').")
            os.remove(source_file)
            print(f"Removed incorrect source file: '{source_file}'")
            return

        # Apply transformations from the notebook
        df['PERIOD'] = f"{year}-{month}"
        df['TNVED'] = df['TNVED'].astype(str).str.zfill(8)
        df['TNVED2'] = df['TNVED'].str.slice(0, 2)
        df['TNVED4'] = df['TNVED'].str.slice(0, 4)
        df['TNVED6'] = df['TNVED'].str.slice(0, 6)
        
        # Ensure STOIM is numeric, handling potential commas
        if df['STOIM'].dtype == 'object':
            df['STOIM'] = df['STOIM'].str.replace(",", "").astype(float)
        
        df['STRANA'] = 'CH'

        month_str = str(month).zfill(2)
        
        # Mirror the trade flow for Russia's perspective
        if flow == 'ИМ':
            df['NAPR'] = 'ЭК'
            output_path = export_path
        elif flow == 'ЭК':
            df['NAPR'] = 'ИМ'
            output_path = import_path
        else:
            print(f"Error: Invalid flow type '{flow}'. Use 'ИМ' or 'ЭК'.")
            return
            
        # Select and order the final columns
        final_columns = [
            'Date of data', 'TNVED', 'NETTO', 'Supplementary Quantity', 
            'Supplementary Unit', 'STOIM', 'PERIOD', 'TNVED2', 'TNVED4', 
            'TNVED6', 'STRANA', 'NAPR'
        ]
        
        # Filter out any columns that might not exist in the source
        final_columns_exist = [col for col in final_columns if col in df.columns]
        
        df_final = df[final_columns_exist]

        output_file = output_path / f'data{year}{month_str}.csv'
        df_final.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        print(f"\nSuccessfully processed and saved data to '{output_file}'")

        os.remove(source_file)
        print(f"Removed source file: '{source_file}'")

    except Exception as e:
        print(f"An error occurred during processing: {e}")


def main():
    """
    Main function to run the data collection and processing script for China.
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

    # Step 2: Process the downloaded file
    if download_successful:
        print(f"Starting file processing...")
        process_downloaded_data(args.year, month_padded, args.flow, args.output_dir)

if __name__ == "__main__":
    # INSTRUCTIONS:
    # 1. Ensure you have Google Chrome installed.
    # 2. Run this script from the terminal. It will open a Chrome window.
    #    Example:
    #    python src/collectors/china-collector.py 2025 7 ИМ --partner 344
    # 3. Manually fill the form, solve CAPTCHA, and download the data.
    # 4. After the download is complete, press Enter in your terminal.
    # 5. The script will then process and save the final data file.
    main()