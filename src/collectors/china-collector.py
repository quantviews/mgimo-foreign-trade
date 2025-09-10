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
    # You might need to adjust preferences depending on your setup
    # prefs = {"download.default_directory": str(download_dir)}
    # options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
    
    try:
        driver.get("http://stats.customs.gov.cn/indexEn")

        # Set Flow (Import/Export)
        flow_map = {'ИМ': 'i', 'ЭК': 'e'}
        driver.find_element(By.ID, f"radio_{flow_map[flow]}").click()

        # Set Period
        driver.find_element(By.ID, "select_year").send_keys(year)
        driver.find_element(By.ID, "select_month").send_keys(month)
        # Ensure 'By month' is checked
        if not driver.find_element(By.ID, "check_month").is_selected():
            driver.find_element(By.ID, "check_month").click()
        
        # Set Partner
        partner_input = driver.find_element(By.ID, "partnerCode")
        partner_input.send_keys(partner_code)

        # Click Enquiry to trigger CAPTCHA
        driver.find_element(By.CSS_SELECTOR, "input[value='Enquiry']").click()
        
        print("\n" + "="*50)
        print("Please solve the CAPTCHA in the browser window.")
        print("After the results page has loaded, press Enter here to continue.")
        print("="*50)
        input()

        # Wait for download button and click it
        wait = WebDriverWait(driver, 30) # 30-second timeout
        download_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a.table-operate-btn.operate-download")))
        download_button.click()

        # Wait for download to complete
        print("Downloading file...")
        while not source_file.exists():
            time.sleep(1)
        
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

    Args:
        year: The year of the data (e.g., '2025').
        month: The month of the data (e.g., '05').
        flow: The trade flow, either 'ИМ' (import) or 'ЭК' (export).
        output_dir: The base directory to save the processed files.
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

        df = df.rename(columns={
            'Commodity code': 'TNVED',
            'US dollar': 'STOIM',
            'Quantity': 'NETTO',
            'Supplimentary Quantity': 'KOL',
            'Supplimentary Unit': 'EDIZM'
        })
        
        # Drop columns that are not needed
        cols_to_drop = ['Commodity', 'Trading partner code', 'Trading partner', 'Unit']
        df = df.drop(columns=[col for col in cols_to_drop if col in df.columns])
        
        # Remove any unnamed columns that sometimes appear in the source file
        df = df.loc[:, ~df.columns.str.startswith('Unnamed')]
        
        df['PERIOD'] = f"{year}-{month}"

        # Format HS codes
        df['TNVED'] = df['TNVED'].astype(str).str.zfill(8)
        df['TNVED2'] = df['TNVED'].str.slice(0, 2)
        df['TNVED4'] = df['TNVED'].str.slice(0, 4)
        df['TNVED6'] = df['TNVED'].str.slice(0, 6)

        df['STOIM'] = df['STOIM'].astype(str).str.replace(",", "").astype(float)
        df['STRANA'] = 'CH'

        month_str = str(month).zfill(2)
        
        # Mirror the trade flow for Russia's perspective
        if flow == 'ИМ':
            df['NAPR'] = 'ЭК'
            output_file = export_path / f'data{year}{month_str}.csv'
        elif flow == 'ЭК':
            df['NAPR'] = 'ИМ'
            output_file = import_path / f'data{year}{month_str}.csv'
        else:
            print(f"Error: Invalid flow type '{flow}'. Use 'ИМ' or 'ЭК'.")
            return

        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"Successfully processed and saved data to '{output_file}'")

        os.remove(source_file)
        print(f"Removed source file: '{source_file}'")

    except Exception as e:
        print(f"An error occurred during processing: {e}")


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
    # 3. When prompted, solve the CAPTCHA in the browser.
    # 4. After the data table appears, press Enter in your terminal.
    # 5. The script will then download and process the file.
    main()