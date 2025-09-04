import asyncio, requests, re, csv
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

DATA_URL = "https://biruni.tuik.gov.tr/disticaretapp/disticaret_ing.zul?param1=4&param2=24&sitcrev=0&isicrev=0&sayac=5902"
YEAR = "2024"
COUNTRY_ID = "75"  # 75 - Россия


async def scraper(playwright, two_digits):
    """
    Функция использует Playwright для того, чтобы запустить headless браузер,
    открыть сайт по ссылке из DATA_URL, затем нажать все нужные радио кнопки
    (числа 1,11, 26 и т.д. это индексы списка элементов на странице) и затем
    запросить список HS8(CN) кодов начинающихся с двух цифр из переменной
    two_digits.

    Выбираются данные за все месяцы года YEAR по коду страны COUNTRY_ID.

    Возвращает список элементов BeautifulSoup.
    """

    browser = await playwright.chromium.launch(headless=True)
    page = await browser.new_page()
    await page.goto(DATA_URL)
    html_content = await page.content()
    doc = BeautifulSoup(html_content, 'html.parser')


    await page.get_by_text("I know country code").click()  # quick fix, refactor later

    for i in [1, 11, 26, 28, 30, 33]:
        id = doc.find_all(class_="z-radio-cnt")[i].get("for")
        await page.check(f'[for= "{id}"]')


    await page.get_by_text(YEAR).click()
    await page.get_by_text("<< All months >>").click()

    text_id = doc.find_all(class_="z-textbox")[0].get("id")
    await page.fill(f"#{text_id}", COUNTRY_ID)

    cn_id = doc.find_all(class_="z-textbox")[3].get("id")
    await page.fill(f"#{cn_id}", two_digits)

    await page.wait_for_selector('button:has-text("Ara")', state='visible')
    await page.get_by_role("button", name="Ara").click()

    await page.wait_for_timeout(2000)
    output = await page.content()

    await browser.close()
    await playwright.stop()

    return BeautifulSoup(output, 'html.parser').find_all(class_="z-listcell")


async def main():
    """
    Функция формирует CSV файл со всеми доступными кодами HS8(CN) запуская
    функцию scraper() для каждого двухзначного кода.
    """
    hs_codes = {}
    pattern = r"\d{8} - .+"  # regex шаблон формата "01234567 - Text..."
    for two_digits in [f"{i:02}" for i in range(100)]:
        async with async_playwright() as playwright:
            bs_elements = await scraper(playwright, two_digits)
            for el in bs_elements:
                s = el.get_text()
                if re.fullmatch(pattern, s):
                    key, value = s.split(" - ", 1)
                    hs_codes[key] = value

    with open("codes.csv", mode="w", newline="", encoding="utf-8") as file:
        fieldnames = ["Code", "Description"]
        writer = csv.DictWriter(file, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        for key, value in hs_codes.items():
            writer.writerow({"Code": key, "Description": value})

if __name__ == "__main__":
    asyncio.run(main())