# Документация по тестам

## Кратко

Проект использует `pytest` для проверки Python-части ETL: объединение обработанных данных, запись в DuckDB, smoke-check финального датасета и контрактные проверки процессоров стран.

Актуальное корректное состояние тестов:

```bash
pytest -q
```

Ожидаемый результат на текущей ветке:

```text
88 passed, 1 xfailed
```

Дополнительно для изменений orchestration/merge:

```text
pytest -q tests/test_sql_quality_checks.py
pytest -q tests/test_nowcast_ingest.py
pytest -q tests/test_merge_processed_data.py -k "MergeCliPaths"
```

## Зависимости

Python-зависимости проекта зафиксированы в `requirements.txt`, включая Prefect 3 и test dependencies:

```bash
python -m pip install -r requirements.txt
```

Зачем нужны пакеты:

- `pytest` — запуск тестов.
- `pandas`, `numpy` — обработка табличных данных.
- `duckdb` — проверка записи и чтения DuckDB.
- `pyarrow` — чтение/запись Parquet в тестах процессоров.
- `beautifulsoup4` — импортируется модулем `turkey_processor`.
- `prefect` — orchestration-слой `src/orchestration/flows.py`.

## Структура

```text
tests/
├── __init__.py
├── conftest.py
├── test_merge_processed_data.py
├── test_nowcast_ingest.py
├── test_processor_contracts.py
└── test_sql_quality_checks.py
```

`tests/conftest.py` задает уникальный `basetemp` внутри `.pytest_tmp/` для каждого запуска. Это нужно, чтобы локальный запуск на Windows не зависел от недоступного системного каталога `%TEMP%\pytest-of-*`.

Важно: `conftest.py` не переназначает `TMP`/`TEMP` глобально, потому что `save_to_duckdb()` специально строит DuckDB во внешнем temp-каталоге. Если отправить DuckDB temp обратно в YandexDisk, Windows/sync-client может держать `.wal` lock и ломать тесты.

## Запуск

Полный прогон:

```bash
pytest -q
```

Подробный прогон:

```bash
pytest tests/ -v
```

Один файл:

```bash
pytest tests/test_merge_processed_data.py -q
pytest tests/test_nowcast_ingest.py -q
pytest tests/test_processor_contracts.py -q
```

Один класс или тест:

```bash
pytest tests/test_merge_processed_data.py::TestSaveToDuckDB -q
pytest tests/test_processor_contracts.py::TestIndiaProcessorContract::test_stoim_from_2026_scaled_by_1000 -q
```

Полезные режимы отладки:

```bash
pytest tests/ --lf
pytest tests/ -x
pytest tests/ -vv
pytest tests/ -v -s --pdb
```

## Что Покрыто

### `test_merge_processed_data.py`

Проверяет совместимый публичный API `src/merge_processed_data.py` и вынесенные функции из `src/core/` и `src/pipelines/`.

- `TestValidateSchema` проверяет обязательную схему, типы данных, `NAPR`, `PERIOD` и отсутствие критичных пропусков.
- `TestGenerateDerivedColumns` проверяет нормализацию `TNVED` и генерацию `TNVED2`, `TNVED4`, `TNVED6`, `TNVED8`.
- `TestNormalizationRules` напрямую проверяет единый модуль правил `src/core/normalization_rules.py`: нормализацию `EDIZM`, country processor aliases и специальные кейсы `KG`, `TONNE`, `BQ`/`БЕККЕРЕЛЬ`.
- `TestLoadTnvedMapping` проверяет загрузку справочников ТН ВЭД из CSV/JSON и нормализацию названий.
- `TestLoadStranaMapping` проверяет загрузку стран и регистронезависимые ключи.
- `TestLoadCommonEdizmMapping` проверяет единицы измерения и алиасы вроде `KG`/`КГ`, `Number of items` → ISO `796`.
- `TestSaveToDuckDB` проверяет запись DataFrame в DuckDB, пустой ввод, чанкинг, overwrite, cleanup временных файлов и сохранность старой базы при ошибке записи.
- `TestIntegration` проверяет связку: derived columns -> schema validation -> DuckDB save.
- `TestSmokeCheckMergedDataset` проверяет smoke-check финального объединенного датасета.
- `TestMergeCliPaths` проверяет CLI-аргумент `--output-db-path` и разрешение относительных/абсолютных путей DuckDB.

### `test_nowcast_ingest.py`

Проверяет Python-ingest R-nowcast из `src/pipelines/nowcast_ingest.py`:

- `TestTransformNowcastToUnified` — только `TYPE='pred'`, unified-колонки, производные `TNVED*`, фильтр `--start-year`, пустой ввод при отсутствии колонок.
- `TestDropNowcastRowsSupersededByFacts` — pred удаляется при совпадении ключа `(PERIOD, STRANA, TNVED, NAPR)` с fact; нормализация `TNVED` при сравнении.
- `TestAppendNowcastData` — чтение parquet, `SOURCE='nowcast'`, `--exclude-countries`, флаги `--no-nowcast` и отсутствующий файл.

### `test_sql_quality_checks.py`

Проверяет read-only SQL quality gate из `src/orchestration/checks.py` на минимальных DuckDB-фикстурах:

- валидная база проходит проверки;
- невалидный `NAPR` валит checks;
- пересечение `TYPE='pred'` с фактом по `(PERIOD, STRANA, TNVED, NAPR)` валит checks;
- отсутствие обязательных таблиц валит checks.

### `test_processor_contracts.py`

Проверяет, что процессоры стран возвращают данные в едином контракте для дальнейшего merge.

`TestCountryProcessorContractLayer` проверяет общий слой `src/core/country_processor_contract.py`: стандартные входы, список выходных колонок и единый post-processing.

Обязательные колонки:

```text
NAPR, PERIOD, STRANA, TNVED, EDIZM, EDIZM_ISO,
STOIM, NETTO, KOL, TNVED2, TNVED4, TNVED6
```

Основные контракты:

- `NAPR` должен быть только `ИМ` или `ЭК`.
- `PERIOD` должен быть datetime.
- `STOIM`, `NETTO`, `KOL` должны быть numeric.
- `TNVED2/4/6` должны быть префиксами `TNVED`.
- Для Китая `STRANA` жестко нормализуется в `CN`, `TNVED` приводится к 8-значному коду источника, а единицы измерения проходят через общий EDIZM-слой.
- Для Индии `PERIOD` строится из `Year`/`Month`, а `EDIZM`/`EDIZM_ISO` нормализуются через общий слой `src/core/normalization_rules.py`.
- Для Индии правило масштаба `STOIM`: до `2026-01` источник уже в тыс. USD; начиная с `2026-01` источник в млн USD и умножается на `1000`.
- Для Турции направление инвертируется относительно турецкого источника: `Export Dollar` становится российским импортом `ИМ`, `Import Dollar` становится российским экспортом `ЭК`.
- Для ФТС CSV зафиксирован текущий контракт и известный gap по `EDIZM_ISO`.

## DuckDB И Windows/YandexDisk

`save_to_duckdb()` учитывает особенности Windows и синхронизируемых папок:

- DuckDB строится во временном каталоге вне целевого YandexDisk-пути.
- Перед копированием выполняется `CHECKPOINT`, чтобы сбросить WAL в основной файл.
- Cleanup `.wal`/`.tmp` после успешной записи выполняется best-effort: если sync-client кратковременно держит lock, это не должно превращать успешную запись в падение.
- При перезаписи существующей базы создается локальная backup-копия, чтобы при ошибке копирования можно было восстановить старую базу.

Если локально остаются `.pytest_tmp*`, `.duckdb.wal` или другие временные файлы, сначала проверьте, что это ignored artifacts. На Windows/YandexDisk они могут удалиться не сразу из-за lock со стороны Python, DuckDB или клиента синхронизации.

## CI

Для Python-тестов есть отдельный GitHub Actions workflow:

```text
.github/workflows/python-tests.yml
```

Он запускается:

- на `push` в `main`, если менялись `src/**/*.py`, `tests/**/*.py` или сам workflow;
- на `pull_request` с такими же path-фильтрами;
- вручную через `workflow_dispatch`.

Workflow использует:

- `ubuntu-latest`;
- Python `3.11`;
- `actions/setup-python@v5` с pip cache;
- установку минимальных test dependencies;
- команду `pytest -q`.

Quarto publish workflow остается отдельно в `.github/workflows/publish.yml` и не смешивается с Python ETL checks.

## Добавление Новых Тестов

Рекомендации:

- Добавляйте unit/smoke-тесты merge-логики в `tests/test_merge_processed_data.py`.
- Добавляйте тесты nowcast-ingest в `tests/test_nowcast_ingest.py`.
- Добавляйте контрактные проверки процессоров стран в `tests/test_processor_contracts.py`.
- Используйте `tmp_path` для файловых фикстур.
- Не используйте реальные `data_raw/`, `data_processed/`, `db/` в тестах, если можно собрать минимальную фикстуру на лету.
- Для новых стран сначала фиксируйте единый output-contract: колонки, типы, `NAPR`, `PERIOD`, `TNVED*`, масштаб `STOIM`.
- Если поведение известно как временно некорректное, помечайте тест `xfail` с явной причиной и планом снятия.

Минимальный шаблон:

```python
def test_specific_contract(tmp_path):
    source = tmp_path / "input.csv"
    source.write_text("...", encoding="utf-8")

    output = tmp_path / "output.parquet"
    process_function(tmp_path, output)

    df = pd.read_parquet(output)
    assert not df.empty
```

## Покрытие

Если установлен `pytest-cov`, можно запускать:

```bash
pytest tests/ --cov=src --cov-report=term-missing
pytest tests/ --cov=src --cov-report=html
```

HTML-отчет будет доступен в `htmlcov/index.html`.

## Смежная Документация

- `docs/refactoring-plan.md` — исторический план технического рефакторинга от 2026-05-06; актуальное состояние проверяйте по коду, тестам и этой документации.
- `docs/orchestration.md` — Prefect 3 flow, повторный merge после nowcast/fizob и SQL quality checks.
- `docs/merge_processed_data-docs.md` — документация по объединению данных и DuckDB.
- `docs/india-processor-docs.md` — детали обработки Индии, включая масштаб `STOIM`.
- `docs/fts_csv_format.md` — формат CSV ФТС.
