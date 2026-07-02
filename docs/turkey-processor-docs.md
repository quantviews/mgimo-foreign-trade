# Data Processor — Обработка данных TUIK

Скрипт для загрузки, нормализации и объединения статистических данных из файлов Excel, скачанных с сайта TUIK (Турецкое статистическое управление).

##  Описание

Обрабатывает файлы структуры: `{path}/{YYYY}/{YYYY}-{MM}.xlsx`

Преобразует данные в единый датасет с колонками:
- **NAPR** — направление (ИМ=импорт, ЭК=экспорт)
- **PERIOD** — период (дата)
- **STRANA** — страна (TR=Турция)
- **TNVED** — код товара (8 знаков)
- **EDIZM** — единица измерения (турецкое название)
- **EDIZM_ISO** — код единицы измерения (ISO)
- **KOL** — количество
- **NETTO** — вес нетто (кг)
- **STOIM** — стоимость (USD)
- **TNVED2, TNVED4, TNVED6** — сокращенные коды товаров

## Быстрый старт

### Установка зависимостей

```bash
pip install pandas openpyxl
```

### Использование из командной строки

```bash
# Базовое использование (2019-текущий год)
python3 turkey_processor.py /path/to/data output_file.parquet

# С указанием периода
python3 turkey_processor.py /path/to/data 2019 2024 output_file.parquet

# Пример
python3 turkey_processor.py /Volumes/storage/Projects/raw_tr_new_gui/ 2020 2025 output_file.parquet
```

### Использование из Python кода

```python
from turkey_processor import load_and_process_data, print_statistics

# Загрузить и обработать данные
df, stats = load_and_process_data(
    base_path="/path/to/data",
    start_year=2020,
    end_year=2024,
    verbose=True,
    show_progress=False
)

# Вывести статистику
print_statistics(stats)

# Работать с датасетом
print(f"Всего строк: {len(df)}")
print(df.head())
```

##  Функции

### `normalize(df: pd.DataFrame) -> pd.DataFrame`

Нормализует данные из одного файла Excel.

**Особенности:**
- Автоматически находит строку с заголовками
- Извлекает год и месяц из данных
- Разделяет импорт и экспорт
- Переименовывает колонки в русские названия
- Добавляет коды TNVED разных уровней (2, 4, 6, 8)
- Преобразует единицы измерения в коды ISO


---

### `find_data_files(base_path, start_year=2019, end_year=None) -> (dict, list)`

Ищет файлы данных в структуре `{YYYY}/{YYYY}-{MM}.xlsx`.

**Возвращает:**
- Словарь найденных файлов: `{(год, месяц): Path}`
- Список отсутствующих месяцев: `[(год, месяц), ...]`

**Пример:**
```python
from data_processor import find_data_files

found, missing = find_data_files("/data", 2020, 2024)
print(f"Найдено: {len(found)}")
print(f"Отсутствует: {len(missing)}")
```

---

### `load_and_process_data(base_path, start_year=2019, end_year=None, verbose=True, show_progress=False) -> (pd.DataFrame, dict)`

Главная функция — загружает, нормализует и объединяет все файлы.

**Параметры:**
- `base_path` (str) — путь к папке с данными
- `start_year` (int) — начальный год (по умолчанию 2019)
- `end_year` (int) — конечный год (по умолчанию текущий)
- `verbose` (bool) — выводить подробные логи
- `show_progress` (bool) — показывать прогресс (зарезервировано для будущих версий)

**Возвращает:**
- DataFrame с объединенными данными
- Словарь статистики:
  ```python
  {
      'total_files': int,        # всего найдено файлов
      'processed_files': int,    # успешно обработано
      'failed_files': list,      # файлы с ошибками
      'missing_months': list,    # отсутствующие месяцы
      'total_rows': int,         # всего строк в датасете
  }
  ```

**Пример:**
```python
from data_processor import load_and_process_data

df, stats = load_and_process_data(
    base_path="/Volumes/storage/Projects/Marcel/raw_tr_new_gui/",
    start_year=2020,
    end_year=2025
)

print(f"Обработано файлов: {stats['processed_files']}")
print(f"Строк в датасете: {stats['total_rows']}")
```

---

### `get_expected_months(start_year=2019, end_year=None) -> list`

Генерирует список ожидаемых месяцев для периода.

**Возвращает:** Список кортежей `[(год, месяц), ...]`

---

### `print_statistics(stats: dict) -> None`

Красиво выводит статистику обработки.

**Пример:**
```
======================================================================
СТАТИСТИКА ОБРАБОТКИ ДАННЫХ
======================================================================

Файлы:
  Найдено файлов:        89
  Успешно обработано:    89
  Ошибок обработки:      0
  Отсутствующих:         2

Данные:
  Всего строк в датасете: 1,234,567
```

##  Примеры использования

### Пример 1: Загрузить данные за один год

```python
from data_processor import load_and_process_data, print_statistics

df, stats = load_and_process_data(
    base_path="/data",
    start_year=2024,
    end_year=2024
)

print_statistics(stats)
```

### Пример 2: Фильтрация по направлению

```python
# Только импорт
imports = df[df['NAPR'] == 'ИМ']
print(f"Импорт: {len(imports)} строк")

# Только экспорт
exports = df[df['NAPR'] == 'ЭК']
print(f"Экспорт: {len(exports)} строк")
```

### Пример 3: Анализ по товарам

```python
# Топ-10 товаров по стоимости
top_goods = df.groupby('TNVED')['STOIM'].sum().nlargest(10)
print(top_goods)
```

### Пример 4: Анализ по периодам

```python
# Сумма по месяцам
monthly = df.groupby('PERIOD')['STOIM'].sum()
print(monthly)
```

### Пример 5: Экспорт в CSV

```python
# Сохранить в CSV
df.to_csv('data.csv', index=False)

# С фильтром (только экспорт 2024)
exports_2024 = df[(df['NAPR'] == 'ЭК') & (df['PERIOD'].dt.year == 2024)]
exports_2024.to_csv('exports_2024.csv', index=False)
```

##  Формат данных

### Единицы измерения (EDIZM_ISO)

Словарь `iso_dict` преобразует турецкие единицы в коды ISO:

| Турецкое | Код ISO | Русское |
|----------|---------|---------|
| KG/ADET | 796 | ШТУКА |
| KG/LİTRE | 112 | ЛИТР |
| KG/M3 | 113 | КУБИЧЕСКИЙ МЕТР |
| KG/M2 | 055 | КВАДРАТНЫЙ МЕТР |
| KG/METR E | 006 | МЕТР |
| KG/KG N | 861 | КИЛОГРАММ АЗОТА |
| (и еще 20+) | | |

Если единица не найдена в словаре, EDIZM_ISO будет `None`.

##  Обработка ошибок

### Отсутствующие файлы

Если файл отсутствует, он добавляется в список `missing_months` и логируется как warning.

### Ошибки при обработке

Если файл не может быть обработан (например, неверный формат), ошибка логируется и файл добавляется в `failed_files`. Обработка продолжается.

### Стандартные ошибки

- **Missing columns** — файл может быть повреждён или иметь неправильный формат

##  Производительность

Примерное время обработки:
- 1 файл (1 месяц): ~0.5 сек
- 12 файлов (1 год): ~6 сек
- 89 файлов (7+ лет): ~45 сек

Использование памяти:
- 1 файл: ~5-10 MB
- 89 файлов объединенные: ~200-300 MB

##  Отладка

Включить DEBUG логирование:

```python
import logging

logging.basicConfig(level=logging.DEBUG)

from data_processor import load_and_process_data
df, stats = load_and_process_data("/data")
```

Проверить структуру одного файла:

```python
import pandas as pd

df = pd.read_excel("2020-01.xlsx")
print("Первые 10 строк:")
print(df.head(10))
print("\nИмена колонок:")
print(df.columns.tolist())
```

## Устранение неисправностей

При возникновении проблем:

1. Проверить формат файлов — должны быть `.xlsx` в структуре `{YYYY}/{YYYY}-{MM}.xlsx`
2. Убедиться что установлены зависимости: `pandas`, `openpyxl`
3. Включить DEBUG логирование для детального анализа
4. Проверить первые 10 строк файла вручную через Excel

