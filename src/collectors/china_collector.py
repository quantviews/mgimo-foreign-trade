import os
import argparse
import sys
from pathlib import Path
import tempfile
import json

# Направление с точки зрения Китая -> значение радио iEType на портале.
# ИМ = импорт Китая (радио "10"), ЭК = экспорт Китая (радио "1").
IE_RADIO = {"ИМ": "10", "ЭК": "1"}

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from collectors._base import get_project_root
from collectors.china_evidence import ingest_export


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

def get_chrome_major_version():
    """Detect the installed Chrome major version so uc pins the matching driver.

    undetected_chromedriver otherwise reuses a cached driver that can target a
    newer Chrome than the one installed, raising SessionNotCreatedException
    (e.g. "driver only supports Chrome 150" while the browser is 149).

    Returns the major version as int, or None if detection fails (uc then
    falls back to its own auto-resolution).
    """
    if os.name != 'nt':
        return None
    import winreg
    for hive in (winreg.HKEY_CURRENT_USER, winreg.HKEY_LOCAL_MACHINE):
        try:
            with winreg.OpenKey(hive, r'Software\Google\Chrome\BLBeacon') as key:
                version = winreg.QueryValueEx(key, 'version')[0]
                return int(str(version).split('.')[0])
        except (FileNotFoundError, OSError, ValueError):
            continue
    return None


def _autofill_form(driver, year: str, month: str, flow: str, partner_code: str):
    """Заполнить надёжно автозаполняемые поля формы (она внутри iframe): период,
    валюта USD, чекбокс «по месяцам» и направление.

    ПАРТНЁРА и отправку запроса намеренно НЕ автоматизируем: портал регистрирует
    страну-партнёра только через свой попап «Selection coding», а не через значение
    поля `outerValue1`. Если проставить его программно, запрос уходит без партнёра
    (портал возвращает 400 и пустую капчу - проверено захватом сети). Поэтому
    партнёра Russia и кнопку Enquiry оставляем пользователю. Best-effort: при сбое
    селектора скрипт сообщит и даст дозаполнить вручную.
    """
    from selenium.webdriver.support.ui import WebDriverWait, Select
    from selenium.webdriver.common.by import By
    wait = WebDriverWait(driver, 30)
    wait.until(lambda d: d.find_elements(By.TAG_NAME, "iframe"))
    driver.switch_to.frame(driver.find_element(By.TAG_NAME, "iframe"))
    try:
        wait.until(lambda d: d.find_elements(By.ID, "year"))
        m = str(int(month))
        Select(driver.find_element(By.ID, "year")).select_by_value(str(int(year)))
        Select(driver.find_element(By.ID, "startMonth")).select_by_value(m)
        Select(driver.find_element(By.ID, "endMonth")).select_by_value(m)

        # Радио (iEType, currencyType) и чекбокс monthFlag - кастомные виджеты:
        # обычный клик Selenium по ним не срабатывает, нативный element.click()
        # через JS работает. Направление проставляем, но просим пользователя
        # проверить (иногда виджет переинициализируется и сбрасывает выбор).
        driver.execute_script(
            "var ie=[...document.getElementsByName('iEType')].find(e=>e.value===arguments[0]);"
            "if(ie) ie.click();"
            "var cu=[...document.getElementsByName('currencyType')].find(e=>e.value==='usd');"
            "if(cu) cu.click();"
            "var mf=document.querySelector('input[name=monthFlag]');"
            "if(mf && !mf.checked) mf.click();",
            IE_RADIO.get(flow, "10"),
        )
        print("  [autofill] Проставлено: период, валюта USD, «по месяцам», направление.")
    finally:
        driver.switch_to.default_content()


def automate_download(year: str, month: str, flow: str, partner_code: str, autofill: bool = False, output_dir=None):
    """
    Opens the Chinese customs stats website, fills the form,
    and waits for the user to solve the CAPTCHA to download the data.
    """
    from selenium.webdriver.support.ui import WebDriverWait
    import undetected_chromedriver as uc
    incoming = Path(output_dir or get_project_root() / 'data_raw' / 'china') / 'incoming'
    incoming.mkdir(parents=True, exist_ok=True)
    download_dir = Path(tempfile.mkdtemp(prefix='customs-', dir=incoming)).resolve()
    source_file = download_dir / 'downloadData.csv'

    options = uc.ChromeOptions()
    options.add_experimental_option("prefs", {"download.default_directory": str(download_dir), "download.prompt_for_download": False})
    # Stealth options to avoid bot detection
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36")

    options.add_argument('--start-maximized')
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--allow-running-insecure-content')
    options.add_argument('--disable-extensions')

    # Pin the driver to the installed Chrome major version. Without this,
    # undetected_chromedriver may reuse a cached driver built for a newer Chrome
    # than the one installed, causing SessionNotCreatedException.
    chrome_major = get_chrome_major_version()
    if chrome_major is not None:
        print(f"Detected Chrome major version: {chrome_major}; pinning ChromeDriver to it.")
    else:
        print("Could not detect Chrome version; letting undetected_chromedriver auto-resolve.")
    driver = uc.Chrome(options=options, version_main=chrome_major)

    try:
        print("Opening browser to the Chinese customs statistics website...")
        driver.get("http://stats.customs.gov.cn/indexEn")

        if autofill:
            # Автозаполнение opt-in. Портал чувствителен к автоматизации: при
            # серии автосеансов его антибот отвечает 412 на входе и 400 на
            # скачивании (пустая капча). Поэтому по умолчанию автозаполнение
            # выключено, а форма заполняется вручную, как в проверенном режиме.
            try:
                _autofill_form(driver, year, month, flow, partner_code)
            except Exception as exc:  # noqa: BLE001
                print(f"[autofill] Автозаполнение не удалось ({exc}). Заполните форму вручную.")
            flow_en = "Import" if flow == "ИМ" else "Export"
            print("\n" + "="*60)
            print("Форма частично заполнена: период, валюта USD, «по месяцам», направление.")
            print("Вручную осталось:")
            print(f"  1) проверьте Flow = {flow_en};")
            print("  2) выберите партнёра Russia кнопкой «Selection coding»;")
            print("  3) нажмите Enquiry, затем Download, решите CAPTCHA и скачайте файл.")
        else:
            print("\n" + "="*60)
            print("Заполните форму вручную: год, месяц, направление, валюта USD,")
            print("«по месяцам», партнёр Russia; нажмите Enquiry, решите CAPTCHA и скачайте.")
        print("Когда 'downloadData.csv' скачается, нажмите Enter здесь.")
        print("="*60)
        input()

        # Wait for download to be present, assuming user has completed it.
        print("Checking for downloaded file...")
        wait = WebDriverWait(driver, 300) # 5-minute timeout for manual process
        wait.until(lambda d: source_file.exists() and not list(download_dir.glob("*.crdownload")))

        # Only public query fields, never cookies, tokens or a full page dump.
        from selenium.webdriver.common.by import By
        snapshot = {"status": "unavailable", "provenance": "form_after_download_not_response_metadata"}
        try:
            driver.switch_to.default_content()
            frames = driver.find_elements(By.TAG_NAME, "iframe")
            if frames:
                driver.switch_to.frame(frames[0])
            snapshot["fields"] = driver.execute_script("""
                const names = ['year','startMonth','endMonth','iEType','currencyType','monthFlag','outerValue1'];
                return Object.fromEntries(names.map(name => [name,
                    [...document.querySelectorAll('[name="'+name+'"], [id="'+name+'"]')]
                    .map(e => ({value:e.value, checked:e.checked ?? null}))]));
            """)
            snapshot["status"] = "captured"
        except Exception as exc:
            snapshot["error_type"] = type(exc).__name__
        finally:
            driver.switch_to.default_content()
        source_file.with_suffix('.query.json').write_text(json.dumps(snapshot,ensure_ascii=False,indent=2),encoding='utf-8')

        print("Download complete.")
        return source_file

    except Exception as e:
        print(f"An error occurred during browser automation: {e}")
        return False
    finally:
        # uc.Chrome may call quit() again from __del__ on Windows, which can throw
        # OSError: [WinError 6] for an already closed handle. Shut down once and
        # then replace instance quit() with a no-op to keep __del__ silent.
        original_quit = driver.quit
        try:
            original_quit()
        except OSError:
            pass
        finally:
            driver.quit = lambda: None

def process_downloaded_data(year, month, flow, output_dir, source_file=None, partner_code='344'):
    """Archive original bytes and validate every row before processing."""
    source_file = Path(source_file) if source_file else get_download_path() / 'downloadData.csv'
    destination, archive = ingest_export(source_file, output_dir, year, month, flow, partner_code)
    print(f'Processed: {destination}; original and query metadata: {archive}')
    return destination


def main():
    """
    Main function to run the data collection and processing script for China.
    """
    project_root = get_project_root()
    default_output = project_root / 'data_raw' / 'china'

    parser = argparse.ArgumentParser(description="Process Chinese customs data.")
    parser.add_argument("year", type=str, help="Year of the data (e.g., 2025)")
    parser.add_argument("month", type=str, help="Month of the data (e.g., 5 or 05)")
    parser.add_argument("flow", type=str, choices=['ИМ', 'ЭК'], help="Flow type: 'ИМ' for Import, 'ЭК' for Export")
    parser.add_argument("--partner", type=str, default="344", help="Partner code (e.g., 344 for Russia).")
    parser.add_argument("--output_dir", type=Path, default=default_output, help="Directory to save processed files.")
    parser.add_argument("--autofill", action="store_true",
                        help="автозаполнить поля формы (период/валюта/направление). "
                             "Опционально: портал чувствителен к автоматизации, по умолчанию выключено.")

    parser.add_argument("--source-file", type=Path, help="Process an existing original CSV without opening a browser.")
    args = parser.parse_args()

    month_padded = args.month.zfill(2)

    print(f"Starting automation for Year: {args.year}, Month: {month_padded}, Flow: {args.flow}, Partner: {args.partner}")

    # Step 1: Automate the download
    download_successful = args.source_file or automate_download(args.year, month_padded, args.flow, args.partner, autofill=args.autofill, output_dir=args.output_dir)

    # Step 2: Process the downloaded file
    if download_successful:
        print(f"Starting file processing...")
        process_downloaded_data(args.year, month_padded, args.flow, args.output_dir, source_file=download_successful, partner_code=args.partner)

if __name__ == "__main__":
    # INSTRUCTIONS:
    # 1. Ensure you have Google Chrome installed.
    # 2. Run this script from the terminal. It will open a Chrome window.
    #    Example:
    #    python src/collectors/china_collector.py 2025 7 ИМ --partner 344
    # 3. Manually fill the form, solve CAPTCHA, and download the data.
    # 4. After the download is complete, press Enter in your terminal.
    # 5. The script will then process and save the final data file.
    main()
