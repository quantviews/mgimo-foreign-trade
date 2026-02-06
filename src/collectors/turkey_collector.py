"""
Модуль позволяет выгружать HS8 коды и сырые данные с сайта института статистики Турции в виде html таблиц. HS8 коды
сохраняются отдельно в виде json файлов.
"""

import argparse, asyncio, re, json, time
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Error
from pathlib import Path
from datetime import datetime


def parse_arguments():
    """
    Обработчик аргументов для запуска модуля из командной строки.

    usage: collector.py [-h] [-c YEAR] [-y YEAR]
    -c YEAR, --codes YEAR - загружает только коды за определенный год
    -y YEAR, --year YEAR - год (от 2005 до текущего)

    Если аргументы не указаны, выводится справка и скрипт завершается.
    """
    current_year = datetime.now().year
    parser = argparse.ArgumentParser(
        description=f"Module downloads data from Turkey Institute of Statistics in year range [2005-{current_year}]"
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
        return year

    parser.add_argument(
        "-y",
        "--year",
        dest="year",
        type=valid_year,
        metavar=f"[2005-{current_year}]",
        help=f"download data for a specific year",
    )

    parser.add_argument(
        "-c",
        "--codes",
        dest="codes",
        type=valid_year,
        metavar=f"[2005-{current_year}]",
        help=f"download codes for a specific year without downloading data",
    )

    args = parser.parse_args()

    # Проверка: если нет ни одного аргумента, показываем help и выходим
    if (not hasattr(args, "year") or args.year is None) and (
        not hasattr(args, "codes") or args.codes is None
    ):
        parser.print_help()
        parser.exit(1)

    return args


async def download_and_save_codes(playwright, year: str) -> dict:
    """
    Функция подключается к сайту института статистики Турции выгружает все возможные коды HS8 за определенный год
    и сохраняет в виде json файла в папке "HS_CODES_DIR" (если папка отсутствует она будет создана)
    :param playwright:
    :param year:
    :return: dict
    """
    browser = await playwright.chromium.launch(headless=True)
    page = await browser.new_page()
    await page.goto(DATA_URL)
    html_content = await page.content()
    doc = BeautifulSoup(html_content, "html.parser")

    print(f"Downloading codes for {year} ...")

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
        print(f"Downloading HS2 {two_digits}; Total: {len(hs_codes)}")

    # сохраняем результат в json-file
    HS_CODES_DIR.mkdir(parents=True, exist_ok=True)
    with open(CODES_FILE_PATH, "w", encoding="utf-8") as file:
        json.dump(hs_codes, file, indent=4)

    print(f"Codes were saved in {CODES_FILE_PATH.name}")

    await browser.close()


async def load_codes(playwright, year: str) -> dict:
    """
    Если файл с кодами отсутствует, функция загружает коды с помощью функции download_and_save_codes(), сохраняет результат
    в json-файл и возвращает словарь с кодами. Если файл существует, то выполняется его проверка, выгружаются коды
    и возвращается словарь с кодами.
    :param year:
    :return hs_codes:
    """
    hs_codes = None

    try:
        if not CODES_FILE_PATH.exists():
            await download_and_save_codes(playwright, year)
        else:
            print("HS8 codes were already downloaded and will be used for downloading data.")
        with CODES_FILE_PATH.open("r", encoding="utf-8") as f:
            hs_codes = json.load(f)

    except Exception as e:
        print(f"Error handling file: {e}")

    # Проверка, что hs_codes обязательно имеет значение перед возвратом
    if hs_codes is None:
        raise RuntimeError("Failed to load codes.")

    return hs_codes


async def collect_data(playwright, year: str, hs_codes: dict):
    """
    Функция подключается к сайту института статистики Турции и выгружает данные за заданный год используя
    ранее выгруженные HS8 коды. Результат сохраняется в виде html таблиц для дальнейшей обработки.
    :param playwright:
    :param year:
    :return:
    """

    browser = await playwright.chromium.launch(headless=True)
    context = await browser.new_context()
    page = await context.new_page()

    await page.goto(DATA_URL)

    # Настроим страницу и получим id нужных полей
    await page.goto(DATA_URL)
    await page.get_by_text("Monthly").click()
    await page.get_by_text(year).click()
    await page.get_by_text("<< All months >>").click()
    await page.get_by_text("I know country code").click()
    await page.get_by_text("I know HS8(CN) code").click()
    await page.get_by_text("Export").click()
    await page.get_by_text("Import").click()
    await page.get_by_text("$(Dollar)").click()

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

    start_t = time.time()
    start_dt = datetime.fromtimestamp(start_t)

    # Проверяем какой последний батч HS8 кодов был выгружен в этом месяце в виде html файла.
    # Подразумевается, что попытка выгрузки была осуществлена и файлы выгружаются последовательно. То есть, если файлы
    # с младшими кодами по какой-то причине были удалены, мы не будем их снова выгружать.

    html_filename_pattern = re.compile(r"\d{8}-\d{8}-20\d{2}\.html")

    HTML_TABLES_DIR.mkdir(parents=True, exist_ok=True)

    files_with_html = [
        p
        for p in HTML_TABLES_DIR.iterdir()
        if p.is_file() and html_filename_pattern.match(p.name)
    ]

    files_with_html = sorted(
        [
            p
            for p in files_with_html
            if datetime.fromtimestamp(p.stat().st_mtime).year == start_dt.year
            and datetime.fromtimestamp(p.stat().st_mtime).month == start_dt.month
        ]
    )

    if len(files_with_html) == 0:
        latest_file = "0"
    else:
        latest_file = files_with_html[-1].name.split("-")[1]
        print(
            f"Most recently downloaded HS8 code for the required year - {latest_file}\nContinue downloading process ..."
        )

    for batch in batches:

        if latest_file < batch[-1]:

            # Заполнение поля с пакетами кодов
            await page.fill(f"#{cn_input_id}", ",".join(batch))

            # Ожидание открытия новой вкладки с отчетом и обработка исключения с таймаутом ответа от сайта:
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
            filename = HTML_TABLES_DIR / f"{batch[0]}-{batch[-1]}-{year}.html"
            filename.write_text(output, encoding="utf-8")
            print(f"{filename.name} is ready")

            # Закрываем вкладку с отчетом
            await new_page.close()

    # Завершение работы браузера
    await context.close()
    await browser.close()


async def main(args):

    if args.codes:
        async with async_playwright() as playwright:
            await load_codes(playwright, YEAR)

    elif args.year:
        async with async_playwright() as playwright:
            hs_codes = await load_codes(playwright, YEAR)
            print("Downloading data ...")
            try:
                await collect_data(playwright, YEAR, hs_codes)
                print("Raw data download completed.")
            except Error as e:
                print(f"Error: {e}\nTry to run the script again a bit later.")


if __name__ == "__main__":
    DATA_URL = "https://biruni.tuik.gov.tr/disticaretapp/disticaret_ing.zul?param1=4&param2=24&sitcrev=0&isicrev=0&sayac=5902"
    COUNTRY_ID = "75"  ## 75 - Россия
    BATCH_SIZE = 25  # максимальное количество кодов в одном отчете - 25
    HS_CODES_DIR = Path.cwd() / "hs_codes_json"

    args = parse_arguments()
    if args.codes is not None:
        YEAR = str(args.codes)
    elif args.year is not None:
        YEAR = str(args.year)

    CODES_FILE_PATH = HS_CODES_DIR / f"turkey_codes_{YEAR}.json"
    HTML_TABLES_DIR = Path.cwd() / "raw_html_tables" / YEAR
    asyncio.run(main(args))
