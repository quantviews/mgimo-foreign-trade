# Занятия по внешнеторговой статистике

Курс: **Международные технологические рынки и технологическое лидерство** (МГИМО)

## Структура

| Файл | Занятие | Описание |
|------|---------|----------|
| `lesson_01_intro.qmd` | 1 | Введение в таможенную статистику, ТН ВЭД, зеркальная статистика |
| `lesson_02_old_new_stats.qmd` | 2 | Старая и новая статистика (до/после января 2022) |
| `lesson_03_duckdb.qmd` | 3 | DuckDB, SQL, аналитические запросы |
| `lesson_04_superset.qmd` | 4 | Superset дашборд, визуализация |

## Практические задания (Python)

Скрипты в `practices/`:

```bash
# Из корня проекта
python lessons/practices/practice_01_intro.py
python lessons/practices/practice_02_old_new.py
python lessons/practices/practice_03_duckdb.py
python lessons/practices/practice_04_superset.py
```

**Требования:** Python 3.8+, duckdb, pandas. Для визуализации: matplotlib.

## Рендер презентаций (Quarto)

```bash
quarto render lessons/lesson_01_intro.qmd
quarto render lessons/lesson_02_old_new_stats.qmd
quarto render lessons/lesson_03_duckdb.qmd
quarto render lessons/lesson_04_superset.qmd
```

Или все сразу:
```bash
quarto render lessons/*.qmd
```

## Superset

Доступ к инстансу Superset с подключённой базой данных будет предоставлен отдельно.

## База данных

Практические скрипты используют `db/unified_trade_data.duckdb`. Создайте её командой:

```bash
python src/merge_processed_data.py --include-comtrade
```
