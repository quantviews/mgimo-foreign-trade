# Актуальная статистика внешней торговли (МГИМО)

Репозиторий содержит ETL-пайплайн для сборки, гармонизации и публикации статистики внешней торговли в DuckDB/Superset.

## Быстрый старт

Установить Python-зависимости и пакет проекта (editable):

```bash
python -m pip install -r requirements.txt
python -m pip install -e .
```

Собрать текущую базу через совместимый CLI:

```bash
python src/merge_processed_data.py --include-comtrade --start-year 2019
```

Запустить orchestration-слой Prefect 3:

```bash
python src/orchestration/flows.py
```

## API-доступ к данным

Тонкий read-only сервис (FastAPI) поверх собранной DuckDB — программный доступ к данным
с токен-авторизацией и аудитом использования. Код — [`api/`](api/), запуск локально:

```bash
python -m pip install -r api/requirements.txt
uvicorn app.main:app --app-dir api --reload   # dev-режим, токен MGIMO_API_DEV_TOKEN
```

Справочник для потребителей (эндпоинты, параметры, Excel/Power Query) —
`docs/api-reference.md`. Интерактивная схема — `GET /docs` (Swagger).

## Основная документация

- `docs/orchestration.md` — порядок полного refresh через Prefect 3, повторный merge после nowcast/fizob и SQL quality checks.
- `docs/merge_processed_data-docs.md` — merge pipeline, DuckDB и CLI-аргументы.
- `docs/data_model.md` — целевая модель `unified_trade_data`, `SOURCE`/`TYPE`, nowcast ingest, справочники и fizob-таблицы.
- `docs/documentation_fizob.md` — расчет индексов физических объемов; физобъемы считаются только по `TYPE = 'fact'`.
- `docs/testing-docs.md` — запуск тестов и покрытые проверки.
- `docs/api-reference.md` — справочник API (эндпоинты, параметры, Excel/Power Query);
  `docs/api-plan.md` и `docs/api-mvp-phase1.md` — план и спецификация MVP.
