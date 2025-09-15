"""
Скрипт подключается к сайту Турецкого Института Статистики, выбирает параметры и загружает данные за определенный год
по всем месяцам и всем доступным кодам за этот год. Используются коды, заранее выгруженные с этого же сайта и хранимые
отдельно в виде json файлов.

Выгрузка за один год занимает примерно 17 минут.
"""


import asyncio
import json
import time
from pathlib import Path
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

DATA_URL = "https://biruni.tuik.gov.tr/disticaretapp/disticaret_ing.zul?param1=4&param2=24&sitcrev=0&isicrev=0&sayac=5902"
YEAR = "2025"
COUNTRY_ID = "75"  # 75 - Россия
BATCH_SIZE = 25 # максимальное количество кодов в одном отчете - 25
DATA_DIR = Path(f"./turkey_data/turkey_html_data_{YEAR}")
DATA_DIR.mkdir(parents=True, exist_ok=True)


async def setup_page(page):
    """Выполнить первоначальную навигацию и настройки фильтров на странице"""
    await page.goto(DATA_URL)
    await page.get_by_text("Monthly").click()
    await page.get_by_text(YEAR).click()
    await page.get_by_text("<< All months >>").click()
    await page.get_by_text("I know country code").click()
    await page.get_by_text("I know HS8(CN) code").click()
    await page.get_by_text("Export").click()
    await page.get_by_text("Import").click()
    await page.get_by_text("$(Dollar)").click()


async def scraper(playwright):
    browser = await playwright.chromium.launch(headless=True)
    context = await browser.new_context()
    page = await context.new_page()

    await page.goto(DATA_URL)

    # Настроим страницу и получим id нужных полей
    await setup_page(page)

    html_content = await page.content()
    doc = BeautifulSoup(html_content, "html.parser")
    textboxes = doc.find_all(class_="z-textbox")

    if len(textboxes) < 3:
        raise RuntimeError("Не найдены необходимые input поля на странице")

    country_input_id = textboxes[0].get("id")
    cn_input_id = textboxes[2].get("id")

    # Вводим страну
    await page.fill(f"#{country_input_id}", COUNTRY_ID)

    # Загружаем коды hs, делим по пакетам
    with open(f"./turkey_data/turkey_codes{YEAR}.json", "r") as f:
        hs_codes = json.load(f)

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
        filename = DATA_DIR / f"{batch[0]}-{batch[-1]}-{YEAR}.html"
        filename.write_text(output, encoding="utf-8")
        print(f"{filename.name} is ready")

        stop_t = time.time()
        elapsed = stop_t - start_t
        print(f"Elapsed time: {int(elapsed // 60):02}:{int(elapsed % 60):02}\n")

        # Закрываем вкладку с отчетом
        await new_page.close()

    # Завершение работы браузера и Playwright
    await context.close()
    await browser.close()


async def main():
    async with async_playwright() as playwright:
        await scraper(playwright)


if __name__ == "__main__":
    start_t = time.time()
    asyncio.run(main())
    stop_t = time.time()
    elapsed = stop_t - start_t
    print(f"Total time: {int(elapsed // 60):02}:{int(elapsed % 60):02}\n")
