# Turkey Collector — Рабочая версия

Скрипт для автоматического скачивания статистических данных с сайта TUIK (Турецкое статистическое управление).

##  Быстрый старт

### Установка

```bash
pip install playwright pandas python-dateutil openpyxl beautifulsoup4
playwright install chromium
```

### Использование

```bash
# Скачать апрель-декабрь 2020 (по 2 месяца параллельно)
python3 turkey_collector.py --path "/Volumes/storage/Projects/Marcel" \
                        --year 2020 \
                        --start-month 4 \
                        --end-month 12 \
                        --concurrent 2

# Скачать один месяц
python3 turkey_collector.py --path "/Volumes/storage/Projects/Marcel" \
                        --year 2020 \
                        --start-month 1 \
                        --end-month 1
```

##  Параметры

| Параметр | По умолчанию | Описание |
|----------|--------------|---------|
| `--path` | - | Путь для сохранения файлов (обязательный) |
| `--year` | - | Год (обязательный) |
| `--start-month` | 1 | Начальный месяц |
| `--end-month` | 12 | Конечный месяц |
| `--kod` | "00000000" | Код продукта для поиска |
| `--concurrent` | 2 | Макс. одновременных загрузок (1-4) |
| `--retries` | 3 | Кол-во попыток при ошибке |

##  Архитектура

### Классы

**`TurkeyCollector`** — основной класс
- `download_month(year, month, kod)` — скачать один месяц
- `download_range(year, start_month, end_month, kod, concurrent)` — скачать диапазон

### Методы

- `_scrape_month()` — скрепинг с управлением браузером
- `_fill_form()` — заполнение и отправка формы
- `_generate_and_download()` — генерирование отчета и скачивание Excel

##  Ключевые особенности

### Надежность
- **Автоматический retry** — до 3 попыток при ошибках сети
- **Экспоненциальная задержка** — 2s, 4s, 8s между попытками
- **Безопасное управление ресурсами** — автоматическое закрытие браузера

### Производительность
- **Асинхронная параллельность** — несколько месяцев одновременно
- **Семафор для контроля** — не перегружает сервер
- **Оптимальные таймауты** — 250ms между действиями, 15s для загрузки отчета

### Логирование
- **INFO** — успешные скачивания и попытки
- **ERROR** — ошибки с полным контекстом
- **DEBUG** — детали каждого действия

##  Структура файлов

```
turkey_collector.py              # Основной скрипт
turkey_collector_examples.py     # Примеры использования
tuik_scraper_debug.py        # Утилиты для отладки
raw_tr_new_gui/
  ├── 2020/
  │   ├── 2020-01.xlsx
  │   ├── 2020-02.xlsx
  │   └── ...
  └── 2021/
      └── ...
```

##  Примеры использования

### Пример 1: Один месяц

```python
import asyncio
from tuik_scraper import TurkeyCollector

async def main():
    scraper = TurkeyCollector(base_path="/Volumes/storage/Projects/Marcel")
    success = await scraper.download_month("2020", 4)
    print(f"Успешно: {success}")

asyncio.run(main())
```

### Пример 2: Диапазон месяцев

```python
async def main():
    scraper = TurkeyCollector(base_path="/Volumes/storage/Projects/Marcel")
    
    results = await scraper.download_range(
        year="2020",
        start_month=4,
        end_month=12,
        concurrent=2  # 2 месяца параллельно
    )
    
    print(f"Успешно: {len(results['success'])}")
    print(f"Ошибок: {len(results['failed'])}")

asyncio.run(main())
```

### Пример 3: Многолетие

```python
async def main():
    scraper = TurkeyCollector(base_path="/Volumes/storage/Projects/Marcel")
    
    for year in ["2019", "2020", "2021"]:
        results = await scraper.download_range(
            year=year,
            start_month=1,
            end_month=12,
            concurrent=2
        )
        print(f"{year}: ✓{len(results['success'])} ✗{len(results['failed'])}")

asyncio.run(main())
```

##  Процесс скрепинга

1. **Загрузка** → сайт TUIK
2. **Выбор типа** → "Ürün / Ürün Grubu - Ülke"
3. **Шаг 1** → выбор "Ülke/Ürün", "Harmonize Sistem", "HS12 (GTIP)"
4. **Шаг 2** → выбор страны (Россия), года, месяца
5. **Выбор кода** → ввод кода продукта
6. **Параметры** → "İhracat", "İthalat", "Miktar 1", "Miktar 2", "USD"
7. **Отчет** → генерирование и скачивание Excel
8. **Сохранение** → в `raw_tr_new_gui/{year}/{year}-{month}.xlsx`

##  Типичные проблемы

### Сайт медленный
**Решение:** увеличить retry
```bash
python3 turkey_collector.py --path "..." --year 2020 --retries 5
```

### Too many files open
**Решение:** снизить параллельность
```bash
python3 turkey_collector.py --path "..." --year 2020 --concurrent 1
```

### Нужна отладка
Запусти с видимым браузером:
```bash
python3 tuik_debug_visual.py download
```

##  Переиспользование

```python
# Один раз создать скрепер
scraper = TurkeyCollector(base_path="/path")

# Использовать много раз
for year in ["2019", "2020", "2021", "2022"]:
    results = await scraper.download_range(year=year)
```

