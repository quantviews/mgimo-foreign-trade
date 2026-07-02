import argparse
import asyncio
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, Tuple

from playwright.async_api import async_playwright, Page, Browser, BrowserContext
import pandas as pd

# Конфигурация
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TurkeyCollector:
    def __init__(
        self,
        base_path: Path,
        url: str = "https://bi.tuik.gov.tr/extensions/tuik-mashup/index.html?report_type=2",
        max_retries: int = 3,
        timeout_ms: int = 30000,
    ):
        self.base_path = Path(base_path)
        self.url = url
        self.max_retries = max_retries
        self.timeout_ms = timeout_ms

    async def download_month(
        self,
        year: str,
        month: int,
        kod: str = "00000000"
    ) -> bool:
        """Скачивает данные для одного месяца с повторами при ошибке."""
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(f"Попытка {attempt}/{self.max_retries}: {year}-{month:02d}")
                await self._scrape_month(year, month, kod)
                logger.info(f"✓ Успешно: {year}-{month:02d}")
                return True

            except asyncio.TimeoutError as e:
                logger.warning(f"Таймаут (попытка {attempt}): {e}")
                if attempt == self.max_retries:
                    logger.error(f"✗ Не удалось скачать {year}-{month:02d} после {self.max_retries} попыток")
                    return False
                await asyncio.sleep(2 ** attempt)  # Экспоненциальная задержка

            except Exception as e:
                logger.error(f"Ошибка при скачивании {year}-{month:02d}: {e}", exc_info=True)
                if attempt == self.max_retries:
                    return False
                await asyncio.sleep(2 ** attempt)

    async def _scrape_month(self, year: str, month: int, kod: str) -> None:
        """Выполняет скрепинг - копируя исходный код максимально близко."""
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            context = await browser.new_context(accept_downloads=True)
            page = await context.new_page()

            # НЕ устанавливаем set_default_timeout как в исходном коде

            # Навигация и заполнение формы
            await self._fill_form(page, year, str(month), kod)

            # Генерация отчета и скачивание
            await self._generate_and_download(page, year, month)

            await context.close()
            await browser.close()

    async def _fill_form(self, page: Page, year: str, month: str, kod: str) -> None:
        """Заполняет форму - копируя исходный код точно."""
        await page.goto(self.url)
        await page.get_by_text("Ürün / Ürün Grubu - Ülke").click()

        buttons = page.get_by_text("Sonraki Adım")
        subm1 = buttons.nth(0)
        subm2 = buttons.nth(1)

        if await subm1.is_visible():
            await subm1.click()

        await page.get_by_text("Ülke/Ürün").click()
        await page.wait_for_timeout(250)
        await page.get_by_text("Harmonize Sistem").click()
        await page.wait_for_timeout(250)
        await page.get_by_text("HS12 (GTIP)").click()

        if await subm2.is_visible():
            await subm2.click()

        await page.get_by_placeholder("Kod ya da Tanım Ara").first.click()
        await page.locator("label", has_text="75 - Rusya Federasyonu").click()

        await page.get_by_text("Seçiniz").nth(0).click()
        await page.get_by_role("option", name=year, exact=True).click()

        await page.wait_for_timeout(1000)

        seciniz = page.get_by_text("Seçiniz")
        await seciniz.last.click()
        await page.get_by_role("option", name=month, exact=True).click()

        await page.get_by_text("Kod Gir").nth(1).click()
        await page.wait_for_timeout(250)

        await page.get_by_text("İhracat", exact=True).click()
        await page.get_by_text("İthalat", exact=True).click()
        await page.get_by_text("Miktar 1 (kilogram)", exact=True).click()
        await page.get_by_text("Miktar 2", exact=True).click()
        await page.get_by_text("USD", exact=True).click()

        cn_input = page.locator("#react-select-4-input")
        await cn_input.click()
        await cn_input.fill(kod)
        await page.wait_for_timeout(300)
        await cn_input.press("Enter")

    async def _wait_visible(self, element, max_attempts: int = 10, timeout: int = 1000) -> bool:
        """Жди пока элемент станет видимым."""
        for attempt in range(max_attempts):
            try:
                if await element.is_visible(timeout=500):
                    return True
            except:
                pass
            await asyncio.sleep(timeout / 1000)
        return False

    async def _generate_and_download(self, page: Page, year: str, month: int) -> None:
        """Генерирует отчет и скачивает Excel - как в исходном коде."""
        download_dir = self.base_path / f"raw_tr_new_gui/{year}"
        download_dir.mkdir(parents=True, exist_ok=True)

        async with page.context.expect_page() as popup_info:
            await page.get_by_role("button", name="Raporu Oluştur").click()

        new_page = await popup_info.value
        await new_page.wait_for_load_state("domcontentloaded")
        await new_page.wait_for_timeout(15000)  # Увеличено с 8000 до 15000

        excel_btn = new_page.get_by_text("Excel", exact=True)

        async with new_page.expect_download() as download_info:
            await excel_btn.click()

        download = await download_info.value
        await download.save_as(str(download_dir / f"{year}-{month:02d}.xlsx"))
        logger.info(f"✓ Скачан: {year}-{month:02d}.xlsx")

        await new_page.close()

    async def _click_with_retry(
        self,
        page: Page,
        selector,
        timeout: int = 5000,
        is_locator: bool = False,
        retries: int = 3
    ) -> None:
        """Клик с автоматическим повтором."""
        for attempt in range(retries):
            try:
                if is_locator:
                    # Уже передан locator
                    element = selector
                elif isinstance(selector, str):
                    # Текст
                    element = page.get_by_text(selector, exact=True)
                else:
                    element = selector

                # Сначала дождемся видимости
                await element.wait_for(timeout=timeout)
                await element.click(timeout=timeout)
                return

            except Exception as e:
                if attempt == retries - 1:
                    logger.error(f"Не удалось кликнуть после {retries} попыток: {e}")
                    raise
                logger.debug(f"Попытка клика {attempt + 1}/{retries} не удалась, повтор...")
                await asyncio.sleep(0.5 * (attempt + 1))

    async def _select_with_retry(
        self,
        page: Page,
        text: str,
        timeout: int = 5000
    ) -> None:
        """Выбирает элемент с повторами."""
        for attempt in range(3):
            try:
                element = page.get_by_text(text, exact=True)
                await element.wait_for(timeout=timeout)
                await element.click(timeout=timeout)
                await page.wait_for_timeout(250)
                return
            except Exception as e:
                if attempt == 2:
                    raise
                logger.debug(f"Повтор выбора '{text}' ({attempt + 1}/3)")
                await asyncio.sleep(0.5)


    async def download_range(
        self,
        year: str,
        start_month: int = 1,
        end_month: int = 12,
        kod: str = "00000000",
        concurrent: int = 2
    ) -> dict:
        """Скачивает данные для диапазона месяцев."""
        results = {"success": [], "failed": []}

        # Ограничиваем количество одновременных задач
        semaphore = asyncio.Semaphore(concurrent)

        async def download_with_semaphore(month):
            async with semaphore:
                success = await self.download_month(year, month, kod)
                if success:
                    results["success"].append(f"{year}-{month:02d}")
                else:
                    results["failed"].append(f"{year}-{month:02d}")

        tasks = [
            download_with_semaphore(month)
            for month in range(start_month, end_month + 1)
        ]

        await asyncio.gather(*tasks)
        return results


async def main():
    parser = argparse.ArgumentParser(description="Сбор данных TUIK со статистикой Турции")
    parser.add_argument("--path", type=str, required=True, help="Путь для сохранения")
    parser.add_argument("--year", type=str, required=True, help="Год")
    parser.add_argument("--start-month", type=int, default=1, help="Начальный месяц")
    parser.add_argument("--end-month", type=int, default=12, help="Конечный месяц")
    parser.add_argument("--kod", type=str, default="00000000", help="Код продукта")
    parser.add_argument("--concurrent", type=int, default=2, help="Кол-во одновременных загрузок")
    parser.add_argument("--retries", type=int, default=3, help="Количество повторов")

    args = parser.parse_args()

    collector = TurkeyCollector(
        base_path=args.path,
        max_retries=args.retries
    )

    results = await collector.download_range(
        year=args.year,
        start_month=args.start_month,
        end_month=args.end_month,
        kod=args.kod,
        concurrent=args.concurrent
    )

    logger.info(f"\n=== ИТОГИ ===")
    logger.info(f"Успешно: {len(results['success'])}")
    logger.info(f"Ошибок: {len(results['failed'])}")
    if results['failed']:
        logger.warning(f"Не удалось скачать: {', '.join(results['failed'])}")


if __name__ == "__main__":
    asyncio.run(main())
