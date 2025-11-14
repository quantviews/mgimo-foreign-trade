# Документация по скрипту объединения данных Comtrade в DuckDB

## Обзор

Скрипт `merge_comtrade_to_duckdb.py` предназначен для объединения множественных Parquet файлов данных Comtrade в единую базу данных DuckDB. Скрипт применяет строгие фильтры для обеспечения качества данных и предотвращения дублирования.

## Основные функции

### 1. Объединение Parquet файлов

**Функция:** `create_duckdb_database(parquet_files: list, db_path: str)`

**Назначение:** Создает единую базу данных DuckDB из множественных Parquet файлов Comtrade.

**Процесс выполнения:**
- Удаляет существующий файл базы данных (если есть)
- Подключается к DuckDB
- Выполняет UNION ALL запрос для всех Parquet файлов
- Создает таблицу `comtrade_data`
- Создает индексы для оптимизации запросов

### 2. Фильтрация данных

**Применяемые фильтры:**
```sql
WHERE customsCode = 'C00'           -- Стандартный таможенный код
  AND motCode = 0                   -- Код способа транспортировки (все способы)
  AND partner2Code = 0             -- Исключение вторичных партнеров
  AND classificationCode = 'H6'     -- Самый детальный уровень HS6
  AND isAggregate = FALSE           -- Исключение агрегированных строк UNSD
  AND isReported = TRUE             -- Только оригинальные данные стран
```

**Обоснование фильтров:**
- **`customsCode = 'C00'`**: Стандартный таможенный код для всех товаров
- **`motCode = 0`**: Включает все способы транспортировки
- **`partner2Code = 0`**: Исключает вторичных партнеров для избежания дублирования
- **`classificationCode = 'H6'`**: Обеспечивает максимальную детализацию на уровне HS6
- **`isAggregate = FALSE`**: Исключает агрегированные данные UNSD
- **`isReported = TRUE`**: Приоритет оригинальным данным стран над оценками

### 3. Преобразование данных

**Преобразование периода:**
```sql
CAST(SUBSTR(CAST(period AS VARCHAR), 1, 4) || '-' || 
     SUBSTR(CAST(period AS VARCHAR), 5, 2) || '-01' AS DATE) as period
```

**Назначение:** Конвертирует формат периода из YYYYMM в стандартный формат даты YYYY-MM-DD.

### 4. Создание индексов

**Создаваемые индексы:**
- `idx_refYear` - по году отчетности
- `idx_refMonth` - по месяцу отчетности  
- `idx_reporterCode` - по коду страны-отчета
- `idx_partnerCode` - по коду страны-партнера
- `idx_flowCode` - по коду торгового потока
- `idx_cmdCode` - по товарному коду

**Цель:** Оптимизация производительности запросов по наиболее частым критериям поиска.

### 5. Контроль качества данных

#### 5.1. Обнаружение дубликатов

**Функция:** Проверка дубликатов по ключевым полям
```sql
SELECT reporterCode, partnerCode, cmdCode, period, flowCode, COUNT(*) as record_count
FROM comtrade_data
GROUP BY reporterCode, partnerCode, cmdCode, period, flowCode
HAVING COUNT(*) > 1
```

**Результат:** Выводит предупреждения о найденных дубликатах с примерами.

#### 5.2. Анализ покрытия данных

**Функция:** Анализ данных по статусу `isReported`
```sql
SELECT isReported, COUNT(*) as record_count, COUNT(DISTINCT reporterCode) as unique_reporters
FROM comtrade_data
GROUP BY isReported
```

**Результат:** Показывает количество записей и уникальных стран-отчетов для каждого типа данных.

### 6. Статистика торговли

#### 6.1. Статистика по годам

**Функция:** Агрегация торговых данных по годам и потокам
```sql
SELECT refYear, flowCode, SUM(primaryValue) as total_value, 
       COUNT(*) as record_count, COUNT(DISTINCT reporterCode) as unique_reporters
FROM comtrade_data
WHERE primaryValue IS NOT NULL AND primaryValue > 0
GROUP BY refYear, flowCode
ORDER BY refYear, flowCode
```

**Вывод:** Детальная статистика экспорта и импорта по годам с количеством записей и уникальных стран-отчетов.

#### 6.2. Общая статистика

**Функция:** Общие суммы экспорта и импорта
```sql
SELECT flowCode, SUM(primaryValue) as total_value, 
       COUNT(*) as record_count, COUNT(DISTINCT reporterCode) as unique_reporters
FROM comtrade_data
WHERE primaryValue IS NOT NULL AND primaryValue > 0
GROUP BY flowCode
ORDER BY flowCode
```

**Вывод:** Общие суммы торговли с разбивкой по потокам.

## Структура данных

### Входные данные
- **Формат:** Parquet файлы в директории `data_raw/comtrade_data/`
- **Структура:** Стандартная схема данных Comtrade
- **Объем:** Множественные файлы по годам/периодам

### Выходные данные
- **Формат:** DuckDB база данных
- **Расположение:** `db/comtrade.db`
- **Таблица:** `comtrade_data`
- **Индексы:** Оптимизированные индексы для быстрого поиска

## Использование

### Базовое использование
```bash
python src/merge_comtrade_to_duckdb.py
```

### Требования
- Python 3.7+
- DuckDB (`pip install duckdb`)
- Доступ к директории `data_raw/comtrade_data/` с Parquet файлами

### Структура директорий
```
project_root/
├── data_raw/
│   └── comtrade_data/
│       ├── file1.parquet
│       ├── file2.parquet
│       └── ...
├── db/
│   └── comtrade.db (создается)
└── src/
    └── merge_comtrade_to_duckdb.py
```

## Логирование и отчетность

### Уровни логирования
- **INFO**: Основные этапы выполнения
- **WARNING**: Предупреждения о дубликатах или проблемах
- **ERROR**: Критические ошибки

### Пример вывода
```
2025-10-24 10:00:00,000 - INFO - Creating DuckDB database at db/comtrade.db
2025-10-24 10:00:01,000 - INFO - Found 5 parquet files
2025-10-24 10:00:02,000 - INFO - Executing merge query with filters...
2025-10-24 10:00:05,000 - INFO - Total rows in DuckDB (HS6 detailed level, non-aggregated, reported): 1,234,567
2025-10-24 10:00:06,000 - INFO - No duplicates found - data is clean!
2025-10-24 10:00:07,000 - INFO - Data coverage by isReported status:
2025-10-24 10:00:07,000 - INFO -   REPORTED: 1,234,567 records from 195 reporters

=== TRADE VALUES BY YEAR ===

Year 2020:
  EXPORT: $15,678,901,234 (123,456 records, 45 reporters)
  IMPORT: $12,345,678,901 (98,765 records, 42 reporters)

=== TOTAL TRADE VALUES ===
EXPORT: $156,789,012,345 (1,567,890 records, 195 reporters)
IMPORT: $123,456,789,012 (1,234,567 records, 195 reporters)
```

## Обработка ошибок

### Типичные ошибки
1. **Отсутствие DuckDB**: `ModuleNotFoundError: No module named 'duckdb'`
   - **Решение**: `pip install duckdb`

2. **Отсутствие Parquet файлов**: `No parquet files found!`
   - **Решение**: Проверить наличие файлов в `data_raw/comtrade_data/`

3. **Ошибки чтения файлов**: Проблемы с форматом или повреждением файлов
   - **Решение**: Проверить целостность Parquet файлов

### Восстановление после ошибок
- Скрипт автоматически удаляет существующую базу данных при запуске
- Все операции выполняются в транзакции DuckDB
- При ошибке база данных не создается

## Производительность

### Оптимизации
- **Индексы**: Создание индексов по ключевым полям
- **Фильтрация**: Применение фильтров на уровне чтения Parquet
- **UNION ALL**: Эффективное объединение данных

### Рекомендации
- Использовать SSD для хранения базы данных
- Обеспечить достаточную оперативную память для больших объемов данных
- Регулярно обновлять статистику базы данных

## Расширение функциональности

### Возможные улучшения
1. **Параллельная обработка**: Обработка файлов в несколько потоков
2. **Сжатие**: Использование сжатия для экономии места
3. **Партиционирование**: Разделение данных по годам/странам
4. **Валидация**: Дополнительные проверки качества данных

### Интеграция с другими скриптами
- Результат используется в `merge_processed_data.py` с флагом `--include-comtrade`
- Совместимость с общей схемой данных проекта
- Поддержка фильтрации по годам и странам

