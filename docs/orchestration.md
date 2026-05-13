# Оркестрация пайплайна через Prefect 3

Этот документ описывает текущий orchestration-слой вокруг существующих Python/R-скриптов. Цель изменения — сделать порядок запуска явным, убрать ручные повторные прогоны и поставить quality gate перед публикацией базы на дашборд.

## Что добавлено

- `requirements.txt` — фиксирует Python-зависимости проекта, включая `prefect>=3.0,<4.0`.
- `src/orchestration/flows.py` — Prefect flow `mgimo-full-refresh`, который пока запускает существующие CLI/R-команды.
- `src/orchestration/checks.py` — SQL quality checks для итоговой DuckDB-базы.
- run manifest — после успешного flow сохраняется JSON с параметрами запуска, версиями входных файлов, итоговой базой и метриками checks.
- `--output-db-path` в `src/merge_processed_data.py` / `src/pipelines/merge_pipeline.py` — позволяет собирать базу не только в `db/unified_trade_data.duckdb`, но и в отдельный артефактный путь.
- `--no-fizob` в `src/merge_processed_data.py` / `src/pipelines/merge_pipeline.py` — позволяет собрать DuckDB без загрузки `fizob_*.parquet` в `fizob_index`.
- `--db-path` и `--output-dir` в `src/nowcast.R` и `src/fizob_queries.R` — R-скрипты больше не привязаны неявно к дефолтной DuckDB-базе.

## Текущий порядок выполнения

Prefect flow намеренно тонкий: бизнес-логика остается в существующих processors, merge pipeline, `nowcast.R` и `fizob_queries.R`. Prefect отвечает за порядок, логи, retry и параметры запуска.

```mermaid
flowchart TD
    A["Country processors"] --> B["Initial merge to DuckDB"]
    B --> C{"run_nowcast?"}
    C -- yes --> D["Rscript src/nowcast.R"]
    C -- no --> E{"run_fizob?"}
    D --> E
    E -- yes --> F["Rscript src/fizob_queries.R"]
    E -- no --> G{"Derived files changed?"}
    F --> G
    G -- yes --> H["Final merge to DuckDB"]
    G -- no --> I["SQL quality checks"]
    H --> I
    I --> J{"run_outlier_detection?"}
    J -- yes --> K["python src/outlier_detection.py"]
    J -- no --> L["Ready for publish/dashboard"]
    K --> L
```

Важный нюанс: если `run_nowcast=True`, первый merge запускается без nowcast (`--no-nowcast`). Это дает R-скрипту nowcast чистую fact-only базу и не подмешивает старый `data_processed/nowcast/nowcast.parquet` из прошлого запуска. Если `run_fizob=True`, первый merge также запускается без fizob (`--no-fizob`), потому что старый `fizob_index` все равно будет заменен свежим расчетом. После пересчета nowcast и/или fizob flow делает повторный merge, чтобы свежие parquet-артефакты попали в итоговую DuckDB.

## Логи и длительные шаги

`run-command` стримит stdout/stderr дочерних Python/R-процессов в Prefect logs построчно. Поэтому после строки `Running: ... src/fizob_queries.R` в актуальной версии flow должны появляться сообщения вида `[fizob] ... | Reading fact rows...`, `[fizob] ... | Building complete monthly grid`, `[fizob] ... | Calculating fizob level TNVED2` и так далее.

`src/fizob_queries.R` может выполняться заметно дольше nowcast: он читает только fact-строки (`TYPE = 'fact'`), строит полную месячную панель по `(STRANA, NAPR, TNVED)`, считает rolling/base-period показатели и несколько уровней агрегации `TNVED2/4/6`. Физобъемы намеренно не считаются по nowcast/pred строкам. Полная месячная сетка, shares и агрегаты `TNVED2/4/6`/`ALL` считаются в DuckDB temp tables; rolling-показатели пока остаются в R через `slider`. Для базы с 2019 года это все равно десятки миллионов строк промежуточной панели, поэтому несколько десятков минут не обязательно означают ошибку.

Методология выбора физической меры в `fizob_queries.R`: если в исходных fact-строках для ряда `(STRANA, NAPR, TNVED)` встречается ровно одна нормальная `EDIZM`, физобъем можно строить по `KOL`. Если единица измерения меняется во времени, неизвестна (`?`, `NA`) или есть наблюдения с `NETTO > 0` при пустом `KOL`, ряд переводится на `NETTO`. Это консервативное правило: временной ряд `KOL` сопоставим только при постоянной единице измерения, а разные `EDIZM` внутри одного ряда делают значения `KOL` методологически неоднородными. Скрипт поэтому считает `n_edizm` по исходным fact-строкам, а не после заполнения `EDIZM` первым доступным значением по группе.

Если после `Running: ... src/fizob_queries.R` больше 5-10 минут нет ни одной строки `[fizob]`, скорее всего запущен старый код flow или R еще не дошел до тела скрипта. В таком случае остановите запуск и перезапустите flow из текущей версии.

## Запуск

Установка зависимостей:

```bash
python -m pip install -r requirements.txt
```

R-зависимости для `run_nowcast=True` и `run_fizob=True`:

```r
install.packages(c("tidyverse", "duckdb", "dfms", "arrow", "vars", "slider"))
```

Для текущей Windows/R установки это можно выполнить из `cmd.exe` так:

```cmd
G:\R\R-4.5.1\bin\Rscript.exe -e "install.packages(c('tidyverse', 'duckdb', 'dfms', 'arrow', 'vars', 'slider'), repos='https://cloud.r-project.org')"
```

`vars` нужен nowcast-шагу через модельную часть `dfms`; если его нет, `src/nowcast.R` падает с ошибкой `нет пакета под названием 'vars'`.

Локальный запуск flow с дефолтными параметрами:

```bash
python src/orchestration/flows.py
```

Этот запуск использует дефолты flow. Он не пересчитывает страновые parquet, nowcast и fizob; он только пересобирает DuckDB из уже существующих parquet-артефактов и запускает SQL quality checks.

## Дефолтное поведение

Дефолтный вызов:

```bash
python src/orchestration/flows.py
```

эквивалентен примерно такому набору параметров:

```python
mgimo_full_refresh(
    process_china=False,
    process_india=False,
    process_turkey=False,
    include_comtrade=True,
    include_nowcast_in_merge=True,
    run_nowcast=False,
    run_fizob=False,
    run_quality_checks=True,
    require_fizob_quality=False,
    run_outlier_detection=False,
    start_year=2019,
    output_db_path=None,
)
```

То есть по умолчанию flow делает:

1. собирает DuckDB через `src/merge_processed_data.py --include-comtrade --start-year 2019`;
2. включает existing nowcast из `data_processed/nowcast/nowcast.parquet`, если файл есть;
3. включает existing fizob parquet из `data_processed/`, если файлы есть;
4. запускает SQL quality checks;
5. пишет результат в `db/unified_trade_data.duckdb`.

По умолчанию flow **не делает**:

- не скачивает новые исходные данные;
- не запускает processors Китая, Индии и Турции;
- не пересчитывает `src/nowcast.R`;
- не пересчитывает `src/fizob_queries.R`;
- не запускает `src/outlier_detection.py`;
- не публикует базу на сервер дашборда.

Это сделано специально: дефолт должен быть быстрым и предсказуемым rebuild/check из текущих артефактов. Любая дорогая работа включается явно.

## Типовые сценарии

### 1. Быстро пересобрать DuckDB из текущих parquet

Если надо только пересобрать базу из уже существующих parquet без пересчета processors, nowcast и fizob:

```bash
python src/orchestration/flows.py
```

Эквивалентный Python-вызов:

```bash
python -c "from src.orchestration.flows import mgimo_full_refresh; mgimo_full_refresh()"
```

Важно: этот сценарий использует existing `data_processed/nowcast/nowcast.parquet` и existing fizob parquet. Для нового месяца это обычно **не финальный production refresh**, потому что derived артефакты могут остаться от прошлого запуска.

### 2. Добавили обработанные parquet за новый месяц

Если `data_processed/*_full.parquet` уже обновлены и надо получить финальную базу для дашборда, запускайте только merge + nowcast + fizob + checks:

```bash
python -c "from src.orchestration.flows import mgimo_full_refresh; mgimo_full_refresh(run_nowcast=True, run_fizob=True, require_fizob_quality=True)"
```

Если Windows не видит `Rscript` в `PATH`, передайте полный путь:

```bash
python -c "from src.orchestration.flows import mgimo_full_refresh; mgimo_full_refresh(run_nowcast=True, run_fizob=True, require_fizob_quality=True, rscript='G:/R/R-4.5.1/bin/Rscript.exe')"
```

Что произойдет:

1. первый merge соберет fact-only базу для R-шагов, без старых nowcast/fizob derived-артефактов;
2. `src/nowcast.R` пересчитает `data_processed/nowcast/nowcast.parquet`;
3. `src/fizob_queries.R` пересчитает fizob parquet по строкам `TYPE = 'fact'`;
4. второй merge соберет финальную DuckDB уже с новым nowcast и fizob;
5. SQL checks проверят структуру, допустимые значения и отсутствие пересечения fact/pred.

Если после проверки база готова к публикации, текущий ручной шаг остается внешним к Prefect: загрузить `db/unified_trade_data.duckdb` на сервер дашборда. Publish-task еще не добавлен.

### 3. Добавили сырой месяц для одной страны

Если появился новый сырой файл только для одной страны, запускайте processor только этой страны, а остальные оставляйте выключенными.

Только Турция:

```bash
python -c "from src.orchestration.flows import mgimo_full_refresh; mgimo_full_refresh(process_turkey=True, run_nowcast=True, run_fizob=True, require_fizob_quality=True, rscript='G:/R/R-4.5.1/bin/Rscript.exe')"
```

Только Индия:

```bash
python -c "from src.orchestration.flows import mgimo_full_refresh; mgimo_full_refresh(process_india=True, run_nowcast=True, run_fizob=True, require_fizob_quality=True, rscript='G:/R/R-4.5.1/bin/Rscript.exe')"
```

Только Китай:

```bash
python -c "from src.orchestration.flows import mgimo_full_refresh; mgimo_full_refresh(process_china=True, run_nowcast=True, run_fizob=True, require_fizob_quality=True, rscript='G:/R/R-4.5.1/bin/Rscript.exe')"
```

### 4. Полный refresh всех стран

Полный refresh нужен после изменений в processor-логике, изменения входного контракта или когда надо принудительно пересобрать все страновые parquet:

```bash
python -c "from src.orchestration.flows import mgimo_full_refresh; mgimo_full_refresh(process_china=True, process_india=True, process_turkey=True, run_nowcast=True, run_fizob=True, require_fizob_quality=True, rscript='G:/R/R-4.5.1/bin/Rscript.exe')"
```

Это самый тяжелый режим; для обычного месячного обновления он нужен только если действительно надо заново прогнать processors.

### 5. Собрать candidate-базу перед публикацией

Чтобы не перезаписывать основную `db/unified_trade_data.duckdb`, можно собрать candidate-файл из текущих parquet-артефактов:

```bash
python -c "from src.orchestration.flows import mgimo_full_refresh; mgimo_full_refresh(output_db_path='db/unified_trade_data_candidate.duckdb')"
```

Если нужно пересчитать nowcast/fizob на candidate-базе, включите derived steps. Flow передаст `--db-path` в оба R-скрипта автоматически:

```bash
python -c "from src.orchestration.flows import mgimo_full_refresh; mgimo_full_refresh(output_db_path='db/unified_trade_data_candidate.duckdb', run_nowcast=True, run_fizob=True, require_fizob_quality=True, rscript='G:/R/R-4.5.1/bin/Rscript.exe')"
```

По умолчанию derived parquet все еще пишутся в рабочие пути `data_processed/nowcast/` и `data_processed/`, чтобы финальный merge мог сразу их подхватить. Если вы задаете кастомные `nowcast_output_dir` или `fizob_output_dir`, убедитесь, что последующий merge читает именно эти артефакты.

## Прямой запуск R-скриптов

Nowcast:

```bash
G:/R/R-4.5.1/bin/Rscript.exe src/nowcast.R --db-path db/unified_trade_data.duckdb --output-dir data_processed/nowcast
```

Fizob:

```bash
G:/R/R-4.5.1/bin/Rscript.exe src/fizob_queries.R --db-path db/unified_trade_data.duckdb --output-dir data_processed
```

Для candidate-базы:

```bash
G:/R/R-4.5.1/bin/Rscript.exe src/nowcast.R --db-path db/unified_trade_data_candidate.duckdb --output-dir data_processed/nowcast
G:/R/R-4.5.1/bin/Rscript.exe src/fizob_queries.R --db-path db/unified_trade_data_candidate.duckdb --output-dir data_processed
```

### 6. Проверить уже собранную DuckDB

SQL checks можно запустить отдельно:

```bash
python -c "from src.orchestration.checks import run_sql_quality_checks; print(run_sql_quality_checks('db/unified_trade_data.duckdb'))"
```

Если fizob-таблицы должны быть обязательными:

```bash
python -c "from src.orchestration.checks import run_sql_quality_checks; print(run_sql_quality_checks('db/unified_trade_data.duckdb', require_fizob=True))"
```

## Параметры flow

- `process_china=False`, `process_india=False`, `process_turkey=False` — запуск страновых processors. По умолчанию выключены, чтобы не делать полный апдейт при каждом rebuild.
- `include_comtrade=True` — добавить Comtrade в merge.
- `include_nowcast_in_merge=True` — включать `data_processed/nowcast/nowcast.parquet` в финальный merge.
- `run_nowcast=False` — пересчитать nowcast через `Rscript src/nowcast.R`.
- `run_fizob=False` — пересчитать физобъемы через `Rscript src/fizob_queries.R`.
- `run_quality_checks=True` — выполнить SQL quality checks после финального merge.
- `require_fizob_quality=False` — считать `fizob_index` и `fizob_index_v` обязательными в SQL checks.
- `run_outlier_detection=False` — запустить `src/outlier_detection.py`.
- `start_year=2019` — передается в merge как `--start-year`.
- `output_db_path=None` — передается в merge как `--output-db-path`; относительный путь считается от корня проекта. Если `None`, используется `db/unified_trade_data.duckdb`.
- `nowcast_output_dir="data_processed/nowcast"` — передается в `src/nowcast.R` как `--output-dir`.
- `fizob_output_dir="data_processed"` — передается в `src/fizob_queries.R` как `--output-dir`.
- `write_manifest=True` — сохранить manifest успешного запуска.
- `manifest_dir="data_processed/manifests"` — куда писать manifest. Flow создает timestamped JSON и обновляет `latest.json`.
- `rscript="Rscript"` — команда запуска R.
- `project_root=None` — корень проекта, по умолчанию определяется автоматически.

## Run Manifest

После успешного запуска flow сохраняет manifest в `data_processed/manifests/`:

- `mgimo_full_refresh_YYYYMMDDTHHMMSSZ.json` — неизменяемый manifest конкретного запуска;
- `latest.json` — копия последнего успешного manifest для быстрого просмотра.

Manifest содержит:

- параметры flow, включая `output_db_path`, `start_year`, флаги nowcast/fizob/checks и путь к `Rscript`;
- git commit, branch и `git status --short` на момент записи manifest;
- путь к итоговой DuckDB и ее версию: размер, `mtime_ns`, UTC-время изменения и fingerprint `size:mtime_ns`;
- версии входных файлов: `data_processed/*.parquet`, nowcast parquet, `fizob_*.parquet`, `db/comtrade.db`;
- метрики SQL checks, если `run_quality_checks=True`.

Пример чтения последнего manifest:

```bash
python -c "import json; print(json.dumps(json.load(open('data_processed/manifests/latest.json', encoding='utf-8')), ensure_ascii=False, indent=2)[:4000])"
```

Если manifest не нужен для экспериментального локального запуска:

```bash
python -c "from src.orchestration.flows import mgimo_full_refresh; mgimo_full_refresh(write_manifest=False)"
```

## SQL quality checks

Quality gate находится в `src/orchestration/checks.py` и запускается как отдельная Prefect task. Проверки читают DuckDB в read-only режиме и валят flow, если находят проблему.

Проверяется:

- наличие `unified_trade_data`, `unified_trade_data_enriched`, `country_reference`, `tnved_reference`;
- наличие обязательных колонок в `unified_trade_data`;
- непустая основная таблица;
- отсутствие `NULL` в `PERIOD`;
- допустимые значения `NAPR`, `TYPE`, `SOURCE`;
- отсутствие пересечения `TYPE='pred'` с фактом по ключу `(PERIOD, STRANA, TNVED, NAPR)`;
- непустые справочники и enriched view;
- при `require_fizob=True` — наличие и непустота `fizob_index`, `fizob_index_v`.

Ручной запуск checks:

```python
from src.orchestration.checks import run_sql_quality_checks

metrics = run_sql_quality_checks("db/unified_trade_data.duckdb")
print(metrics)
```

## Что еще остается сделать

Текущий слой уже фиксирует порядок и убирает ручную ошибку "забыли второй merge". Следующие полезные шаги:

- добавить publish-task: atomic upload DuckDB на сервер дашборда после успешных SQL checks;
- позже перенести запись nowcast/fizob внутрь DuckDB builder, чтобы полностью убрать parquet-cycle.
