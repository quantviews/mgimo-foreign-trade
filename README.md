# Актуальная статистика внешней торговли (МГИМО)

Репозиторий содержит ETL-пайплайн для сборки, гармонизации и публикации статистики внешней торговли в DuckDB/Superset.

## Быстрый старт

Установить Python-зависимости:

```bash
python -m pip install -r requirements.txt
```

Собрать текущую базу через совместимый CLI:

```bash
python src/merge_processed_data.py --include-comtrade --start-year 2019
```

Запустить orchestration-слой Prefect 3:

```bash
python src/orchestration/flows.py
```

## Основная документация

- `docs/orchestration.md` — порядок полного refresh через Prefect 3, повторный merge после nowcast/fizob и SQL quality checks.
- `docs/merge_processed_data-docs.md` — merge pipeline, DuckDB и CLI-аргументы.
- `docs/data_model.md` — целевая модель `unified_trade_data`, `SOURCE`/`TYPE`, nowcast ingest, справочники и fizob-таблицы.
- `docs/documentation_fizob.md` — расчет индексов физических объемов; физобъемы считаются только по `TYPE = 'fact'`.
- `docs/testing-docs.md` — запуск тестов и покрытые проверки.
