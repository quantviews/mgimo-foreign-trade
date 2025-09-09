import asyncio, json, time
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

DATA_URL = "https://biruni.tuik.gov.tr/disticaretapp/disticaret_ing.zul?param1=4&param2=24&sitcrev=0&isicrev=0&sayac=5902"
YEAR = "2025"
COUNTRY_ID = "75"  # 75 - Россия


async def scraper(playwright):
    """
    Функция загружает ранее собранные коды, запрашивает данные пакетами из 25 кодов.
    Результат сохраняется в виде файла "{batch[0]}-{batch[-1]}-{YEAR}.html"
    """
    browser = await playwright.chromium.launch(headless=True)
    context = await browser.new_context()
    page = await context.new_page()
    await page.goto(DATA_URL)
    html_content = await page.content()
    doc = BeautifulSoup(html_content, "html.parser")

    await page.get_by_text("Monthly").click()
    await page.get_by_text(YEAR).click()
    await page.get_by_text("<< All months >>").click()
    await page.get_by_text("I know country code").click()
    await page.get_by_text("I know HS8(CN) code").click()

    text_id = doc.find_all(class_="z-textbox")[0].get("id")
    await page.fill(f"#{text_id}", COUNTRY_ID)

    cn_id = doc.find_all(class_="z-textbox")[2].get("id")

    await page.get_by_text("Export").click()
    await page.get_by_text("Import").click()
    await page.get_by_text("$(Dollar)").click()

    with open(f"./turkey_data/turkey_codes{YEAR}.json") as file:
        hs_codes = json.load(file)

    keys = list(hs_codes.keys())  # Получаем список ключей из словаря
    batches = [
        keys[i : i + 25] for i in range(0, len(keys), 25)
    ]  # Делим список ключей на подсписки по 5 элементов
    for batch in batches:
        start_t = time.time()
        await page.fill(f"#{cn_id}", ",".join(batch))
        async with context.expect_page() as new_page_info:
            await page.wait_for_selector(
                'button:has-text("Make Report")', state="visible"
            )
            await page.get_by_role("button", name="Make Report").click(delay=300)
        new_page = await new_page_info.value
        await new_page.wait_for_load_state("networkidle")
        output = await new_page.content()
        with open(
            f"./turkey_data/{batch[0]}-{batch[-1]}-{YEAR}.html", "w", encoding="utf-8"
        ) as file:
            file.write(output)
            print(f"{batch[0]}-{batch[-1]}-{YEAR}.html is ready")

        stop_t = time.time()
        min = (stop_t - start_t) // 60
        sec = (stop_t - start_t) % 60
        await new_page.close()
        print(f"{int(min):02}:{int(sec):02} elapsed\n")

    await browser.close()
    await playwright.stop()

    # return BeautifulSoup(output, "html.parser").find("table")


async def main():
    async with async_playwright() as playwright:
        await scraper(playwright)


if __name__ == "__main__":
    asyncio.run(main())
