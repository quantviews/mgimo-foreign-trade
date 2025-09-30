"""
Модуль позволяет выгружать HS8 коды и сырые данные с сайта института статистики Турции в виде html таблиц. HS8 коды
сохраняются отдельно в виде json файлов.
"""

import argparse, asyncio, re, json, time
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from pathlib import Path
from datetime import datetime


def parse_arguments():
    """
    Обработчик аргументов для запуска модуля из командной строки.

    usage: new_collector.py [-h] [-c] year
    -c, --codes - загружает только коды за определенный год

    :return: возвращает список аргументов для запуска модуля
    """
    current_year = datetime.now().year
    parser = argparse.ArgumentParser(
        description="Module downloads data from Turkey Institute of Statistics"
    )

    def valid_year(value):
        if not value.isdigit():
            raise argparse.ArgumentTypeError(
                f"Year should be a number in range 2005-{current_year}"
            )
        year = int(value)
        if year < 2005 or year > current_year:
            raise argparse.ArgumentTypeError(
                f"Year should be a number in range 2005-{current_year}"
            )
        return value

    # Позиционные аргументы (обязательные)

    parser.add_argument(
        "year",
        type=valid_year,
        metavar=f"[2005-{current_year}]",
        help=f"year (from 2005 to {current_year})",
    )

    # Опциональные (флаги, опции)
    parser.add_argument(
        "-c",
        "--codes",
        action="store_true",
        help="download only codes for a specific year",
    )

    # Парсинг аргументов
    args = parser.parse_args()
    return args


async def download_codes(playwright, year: str) -> dict:
    """
    Функция подключается к сайту института статистики Турции и выгружает все возможные коды HS8 за определенный год
    :param playwright:
    :param year:
    :return: dict
    """
    browser = await playwright.chromium.launch(headless=True)
    page = await browser.new_page()
    await page.goto(DATA_URL)
    html_content = await page.content()
    doc = BeautifulSoup(html_content, "html.parser")

    for i in [1, 11, 26, 28, 30]:
        id = doc.find_all(class_="z-radio-cnt")[i].get("for")
        await page.check(f'[for= "{id}"]')

    await page.get_by_text(year).click()
    await page.get_by_text("<< All months >>").click(delay=300)

    text_id = doc.find_all(class_="z-textbox")[0].get("id")
    await page.fill(f"#{text_id}", COUNTRY_ID)

    hs_codes = {}
    pattern = r"\d{8} - .+"  # regex шаблон формата "01234567 - Text..."
    cn_id = doc.find_all(class_="z-textbox")[3].get("id")

    # Ждём загрузки кнопки "Ara" после клика
    for two_digits in [f"{i:02}" for i in range(1, 100)]:
        await page.fill(f"#{cn_id}", two_digits)
        await page.get_by_role("button", name="Ara").click(delay=300, timeout=2000)
        await page.wait_for_timeout(2000)
        output = await page.content()
        bs_elements = BeautifulSoup(output, "html.parser").find_all(class_="z-listcell")
        for el in bs_elements:
            s = el.get_text()
            if re.fullmatch(pattern, s):
                key, value = s.split(" - ", 1)
                hs_codes[key] = value
        print(f"HS2 {two_digits}; {len(hs_codes)} codes loaded")

    await browser.close()

    return hs_codes


async def load_codes(playwright, year: str) -> dict:
    """
    Если файл с кодами отсутствует, функция загружает коды с помощью функции download_codes(), сохраняет результат
    в json-файл и возвращает словарь с кодами. Если файл существует, то выполняется его проверка, выгружаются коды
    и возвращается словарь с кодами.
    :param year:
    :return hs_codes:
    """
    hs_codes = None
    hs_codes_dir = Path.cwd() / "hs_codes_json"
    codes_file_path = hs_codes_dir / f"turkey_codes{year}.json"

    try:
        if not codes_file_path.exists():
            hs_codes_dir.mkdir(parents=True, exist_ok=True)
            print(f"Downloading codes for {year}...")
            hs_codes = await download_codes(playwright, year)
            with open(codes_file_path, "w", encoding="utf-8") as file:
                json.dump(hs_codes, file, indent=4)
            print(f"Codes were saved in {codes_file_path.name}")

        else:
            try:
                with open(codes_file_path, "r", encoding="utf-8") as f:
                    hs_codes = json.load(f)
                print(f"Will use previously downloaded HS8 codes for {year}.")

            except (json.JSONDecodeError, IOError):
                print(
                    f"Couldn't process the file with codes for {year}. Downloading again..."
                )
                hs_codes = await download_codes(playwright, year)
                with open(codes_file_path, "w", encoding="utf-8") as file:
                    json.dump(hs_codes, file, indent=4)
                print(f"Codes were saved in {codes_file_path.name}")

    except Exception as e:
        print(f"Error handling file: {e}")

    # Проверка, что hs_codes обязательно имеет значение перед возвратом
    if hs_codes is None:
        raise RuntimeError(f"Failed to load or download codes for {year}")

    return hs_codes


async def setup_page(page, year: str):
    """
    Функция производит первоначальную навигацию и настройки фильтров на странице перед началом выгрузки данных
    :param page:
    :return:
    """
    await page.goto(DATA_URL)
    await page.get_by_text("Monthly").click()
    await page.get_by_text(year).click()
    await page.get_by_text("<< All months >>").click()
    await page.get_by_text("I know country code").click()
    await page.get_by_text("I know HS8(CN) code").click()
    await page.get_by_text("Export").click()
    await page.get_by_text("Import").click()
    await page.get_by_text("$(Dollar)").click()


async def collect_data(playwright, year: str, html_tables_dir, hs_codes: dict):
    """
    Функция подключается к сайту института статистики Турции и выгружает данные за заданный год используя
    ранее выгруженные HS8 коды. Результат сохраняется в виде html таблиц для дальнейшей обработки.
    :param playwright:
    :param year:
    :param html_tables_dir:
    :return:
    """
    browser = await playwright.chromium.launch(headless=True)
    context = await browser.new_context()
    page = await context.new_page()

    await page.goto(DATA_URL)

    # Настроим страницу и получим id нужных полей
    await setup_page(page, year)

    html_content = await page.content()
    doc = BeautifulSoup(html_content, "html.parser")
    textboxes = doc.find_all(class_="z-textbox")

    if len(textboxes) < 3:
        raise RuntimeError("Can't find required input fields on page")

    country_input_id = textboxes[0].get("id")
    cn_input_id = textboxes[2].get("id")

    # Вводим страну
    await page.fill(f"#{country_input_id}", COUNTRY_ID)

    # Подготавливаем пакеты кодов для загрузки

    keys = list(hs_codes.keys())
    batches = [keys[i : i + BATCH_SIZE] for i in range(0, len(keys), BATCH_SIZE)]

    for batch in batches:
        start_t = time.time()

        # Заполнение поля с пакетами кодов
        await page.fill(f"#{cn_input_id}", ",".join(batch))

        # Ожидание открытия новой вкладки с отчетом
        async with context.expect_page() as new_page_info:
            await page.wait_for_selector(
                'button:has-text("Make Report")', state="visible"
            )
            await page.get_by_role("button", name="Make Report").click(delay=300)

        new_page = await new_page_info.value

        # Ожидание полной загрузки содержимого
        await new_page.wait_for_load_state("networkidle")
        output = await new_page.content()

        # Сохранение HTML файла с отчетом
        filename = html_tables_dir / f"{batch[0]}-{batch[-1]}-{year}.html"
        filename.write_text(output, encoding="utf-8")
        print(f"{filename.name} is ready")

        stop_t = time.time()
        elapsed = stop_t - start_t
        print(f"Elapsed time: {int(elapsed // 60):02}:{int(elapsed % 60):02}\n")

        # Закрываем вкладку с отчетом
        await new_page.close()

    # Завершение работы браузера
    await context.close()
    await browser.close()


async def main():
    args = parse_arguments()

    if args.codes:
        async with async_playwright() as playwright:
            await load_codes(playwright, args.year)

    else:

        html_tables_dir = Path.cwd() / "raw_html_tables" / args.year
        html_tables_dir.mkdir(parents=True, exist_ok=True)

        async with async_playwright() as playwright:
            hs_codes = await load_codes(playwright, args.year)
            print("\nDownloading data...\n")
            await collect_data(playwright, args.year, html_tables_dir, hs_codes)

        print("Raw data download completed.")


if __name__ == "__main__":
    DATA_URL = "https://biruni.tuik.gov.tr/disticaretapp/disticaret_ing.zul?param1=4&param2=24&sitcrev=0&isicrev=0&sayac=5902"
    COUNTRY_ID = "75"  ## 75 - Россия
    BATCH_SIZE = 25  # максимальное количество кодов в одном отчете - 25
    asyncio.run(main())
