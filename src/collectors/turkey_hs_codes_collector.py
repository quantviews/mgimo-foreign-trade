"""
V1.1 changelog
    - Переписаны функции scraper() и main(). Теперь headless браузер закрывается только после того, как все коды
    за определенный год выгружены.
    - Выгрузку кодов теперь можно делать по списку годов.
    - Теперь коды сохраняются в json файле по каждому году отдельно
"""

import asyncio, re, json
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

DATA_URL = "https://biruni.tuik.gov.tr/disticaretapp/disticaret_ing.zul?param1=4&param2=24&sitcrev=0&isicrev=0&sayac=5902"
YEAR_RANGE = ["2013"]
COUNTRY_ID = "75"  # 75 - Россия


async def scraper(playwright, YEAR):
    """
    Функция использует Playwright для того, чтобы запустить headless браузер,
    открыть сайт по ссылке из DATA_URL, затем нажать все нужные радио кнопки
    (числа 1,11, 26 и т.д. это индексы списка элементов на странице) и затем
    запросить список HS8(CN) кодов

    Выгружаются данные за все месяцы года YEAR по коду страны COUNTRY_ID.

    Возвращает список элементов в формате 'dict'.
    """

    browser = await playwright.chromium.launch(headless=True)
    page = await browser.new_page()
    await page.goto(DATA_URL)
    html_content = await page.content()
    doc = BeautifulSoup(html_content, "html.parser")

    for i in [1, 11, 26, 28, 30]:
        id = doc.find_all(class_="z-radio-cnt")[i].get("for")
        await page.check(f'[for= "{id}"]')

    await page.get_by_text(YEAR).click()
    await page.get_by_text("<< All months >>").click(delay=300)

    text_id = doc.find_all(class_="z-textbox")[0].get("id")
    await page.fill(f"#{text_id}", COUNTRY_ID)

    hs_codes = {}
    pattern = r"\d{8} - .+"  # regex шаблон формата "01234567 - Text..."
    cn_id = doc.find_all(class_="z-textbox")[3].get("id")

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
        print(f"{YEAR} - {two_digits} - length {len(hs_codes)}")  ## testing

    await browser.close()
    await playwright.stop()

    return hs_codes


async def main(YEAR_RANGE):
    """
    Функция запускает scraper() для каждого года из списка YEAR_RANGE, получает словарь кодов и создает json файл
    отдельно по каждому году.
    """
    for YEAR in YEAR_RANGE:

        async with async_playwright() as playwright:
            data = await scraper(playwright, str(YEAR))
        with open(f"turkey_codes_{YEAR}.json", mode="w", encoding="utf-8") as file:
            json.dump(data, file, indent=4)


if __name__ == "__main__":

    asyncio.run(main(YEAR_RANGE))
