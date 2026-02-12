# Документация и инструкция по использованию `turkey_collector.py` и `turkey_processor.py`

## 1. Назначение скриптов

### `turkey_collector.py`
Скрипт предназначен для автоматического сбора **сырых данных** по внешней торговле между Росией и Турцией с сайта института статистики Турции ([biruni.tuik.gov.tr](https://biruni.tuik.gov.tr/disticaretapp/disticaret_ing.zul?param1=4&param2=24&sitcrev=0&isicrev=0&sayac=5902")) . 
Скрипт открывает страницу с формой для запроса данных, заполняет необходимые поля, отправляет запрос и собирает следующее:

- список и описание кодов ТНВЭД сохраняются в виде json-файла
- страницы с данными по кодам ТНВЭД сохраняются в виде html-файлов

### `turkey_processor.py`
Скрипт предназначен для **обработки, проверки качества и объединения** всех сырых файлов, собранных `turkey_collector.py`. Он сканирует директорию с html-файлами, извлекает данные, консолидирует их результирующую таблицу и приводит эту таблицу к гармонизированному виду на основании [модели данных](https://github.com/quantviews/mgimo-foreign-trade/blob/main/docs/data_model.md). Результат сохраняется в виде parquet файла.

## 2. Использование

[Схема процесса сбора и обработки данных](https://github.com/quantviews/mgimo-foreign-trade/blob/main/fig/turkey-data-collection.png)

### Шаг 1: Сбор сырых данных с `turkey_collector.py`


```
% python turkey_collector.py [-h] [-y [2005-...]] [-c [2005-...]]
```

1.  **Загрузка кодов и сырых данных** (автоматически использует существующие коды, если файл уже есть):
    ```
    % python turkey_collector.py -y [year] 
    ```
    Скрипт проверяет наличие файла `turkey_codes[year].json`. Если файл существует, использует его. Если нет - загружает коды, затем собирает данные.

2.  **Загрузка только кодов** (без сбора данных):
    ```
    % python turkey_collector.py -c [year]
    ```

**Параметры:**
- `-y [2005-...], --year [2005-...]` : загружает ТНВЭД коды (если они не были скачаны ранее), затем загружает данные за указанный год.
- `-c [2005-...], --codes [2005-...]`: загрузить только коды, без сбора данных. Если коды уже были выгружены, скрипт останавливается.
- `-h, --help`: показывает информационное сообщение и останавливается.

### Шаг 2: Обработка данных с `turkey_processor.py`

1.  **Обработка данных за определенный год** Пользователь запускает скрипт указывая необходимый год.
    ```
    % python turkey_processor.py 2025
    ```
2.  **Обработка всех доступных данных** Пользователь запускает скрипт с ключом -a
    ```
    % python turkey_processor.py -a
    ```

## 3. Требования

**Cторонние Python-библиотеки:**
- `playwright` - для автоматизации браузера
- `pandas` - для работы с данными
- `numpy` - для численных операций
- `bs4` (BeautifulSoup) - для парсинга HTML

**Установка зависимостей:**
```
% pip install playwright pandas numpy beautifulsoup4
% playwright install chromium
```

## 4. Особенности работы

### Обработка таймаутов
- При проблемах с загрузкой данных скрипт выводит сообщение о возникшей ошибке и останавливается.
- Если запустить скрипт снова, он проверит для какого кода данные уже были недавно выгружены и продолжит выгрузку со следующего кода.

### Повторные запуски
- Если файл с кодами уже существует, он не будет скачиваться снова (кроме случая повреждения файла)
- Уже загруженные HTML файлы не перезаписываются автоматически, если они были выгружены в текущем месяце. Более старые файлы будут перезаписаны.
- Для того чтобы загрузить коды или данные заново, удалите соответствующие файлы вручную.

## 5. Связанные файлы и директории

**Скрипты:**
-   **`src/collectors/turkey_collector.py`**: Скрипт для выгрузки кодов и сырых данных.
-   **`src/collectors/turkey_processor.py`**: Скрипт для обработки и объединения сырых данных.

**Директории с данными:**
-   **`data_raw/turkey/raw_html_tables/YYYY/`**: Директория для хранения сырых данных (HTML страниц) за год YYYY
    - Формат файлов: `XXXXXX-YYYYYY-YYYY.html` (диапазон кодов-год)
-   **`data_raw/turkey/hs_codes_json/`**: Директория для JSON-файлов с HS8 кодами
    - Формат файлов: `turkey_codesYYYY.json`
-   **`data_processed/turkey/`**: Директория для обработанных данных
    - **`tr_full.parquet`**: Финальный, обработанный файл с данными

## 6. Примеры использования

### Пример 1: Выгрузить только ТНВЭД коды за 2025 год:
```
% python turkey_collector.py -c 2025
Downloading codes for 2025 ...
Downloading HS2 01; Total: 26
Downloading HS2 02; Total: 70
Downloading HS2 03; Total: 217
...
Downloading HS2 97; Total: 8263
Downloading HS2 98; Total: 8263
Downloading HS2 99; Total: 8265
Codes were saved in turkey_codes_2025.json
%
```

### Пример 2: Скачать данные используя ранее выгруженные коды:
```
% python3.11 ./turkey_collector.py -y 2025
HS8 codes were already downloaded and will be used for downloading data.
Downloading data ...
01012100-01064900-2025.html is ready
01069000-02071440-2025.html is ready
...
96140090-97019200-2025.html is ready
97019900-99309900-2025.html is ready
Raw data download completed.
% 
```

### Пример 3: Повторный запуск после сбоя
```
% python turkey_collector.py -y 2025
HS8 codes were already downloaded and will be used for downloading data.
Downloading data ...
01012100-01064900-2025.html is ready
01069000-02071440-2025.html is ready
02071450-03021120-2025.html is ready
03021180-03028170-2025.html is ready
Error: Timeout 30000ms exceeded while waiting for event "page"
Try to run the script again a bit later.

% python turkey_collector.py -y 2025
HS8 codes were already downloaded and will be used for downloading data.
Downloading data ...
Most recently downloaded HS8 code for the required year - 03028170
Continue downloading process ...
03028200-03034390-2025.html is ready
03034410-03039900-2025.html is ready
03044100-03054100-2025.html is ready
...
```

### Пример 4: Обработка данных
```
# Обработать данные за 2025 год
% python src/collectors/turkey_processor.py 2025

# Обработать все доступные данные
% python src/collectors/turkey_processor.py -a
```

## 7. Устранение проблем

### Проблема: Таймаут при загрузке страницы
**Решение:** Скрипт поддерживает дозагрузку данных. Если проблема повторяется:
- Проверьте интернет-соединение.
- Убедитесь, что сайт доступен.
- Попробуйте запустить снова - скрипт продолжит с того места, где остановился.
- Если проблема повторяется, возможно ведутся технические работы на сайте с данными. Попробуйте повторить загрузку позже.


### Проблема: Поврежденный файл с кодами
**Решение:** Скрипт автоматически обнаружит проблему и перезагрузит коды. Или удалите файл вручную и запустите выгрузку скрипта снова.

