# План: разделение trade / fizob на две DuckDB-базы

> ## ⛔ ВЕРДИКТ (2026-07-30): НЕ ПРИНЯТО — держим одну БД
>
> **Причина:** торговые и fizob-чарты живут в одном дашборде с общей панелью фильтров
> (Superset не поддерживает разные фильтры на разных вкладках). При двух БД общий фильтр
> пришлось бы гонять через scoping по имени колонки на обе БД — с заусенцами (список значений
> из одного датасета, точное совпадение имён/типов, legacy filter-box вообще не кроссится).
> Постоянная боль на смешанном дашборде.
>
> Выгоды деления при этом почти нулевые: **размер уже решён** (4.6 → 1.5 ГБ); **границы под
> API не требуют физического деления** (сервис отдаёт `/trade` и `/fizob` из одного файла);
> **независимый рефреш fizob** достижим точечной пересборкой в том же файле.
>
> **Что делаем вместо:** один `unified_trade_data.duckdb`; логическое разделение — на уровне
> API-сервиса (роутинг + токен + Excel через REST/OData или Postgres-wire) и опционально через
> схемы внутри файла (`trade.*` / `fizob.*`, фильтры всё равно кроссятся); независимое
> обновление fizob — отдельной командой.
>
> **Пересмотреть, если** fizob станет отдельным дашбордом (свои фильтры) или уедет на
> отдельный сервер / под отдельные права хранения. Тогда план ниже — в силе.

---

> Составлено: 2026-07-30. Статус: **план, НЕ принят (см. вердикт выше)** — оставлен как справка.
> Контекст: БД используется в Superset; в перспективе — доступ по API (токен-авторизация,
> работа из Excel), остаёмся на DuckDB. Кросс-доменных запросов trade×fizob нет.

## Зачем

`unified_trade_data` (торговые факты, 7.4M строк) и физобъёмы (`fizob_*`, 39.5M строк,
~70% файла) — два разных дата-продукта: сырьё vs производный индекс, считаются разными
пайплайнами (Python-мёрдж vs R-методология), обновляются в разном каденсе, у fizob чаще
меняется методология. На этапе обслуживания они независимы. Плюсы разделения:
независимый рефреш, лёгкий торговый файл (~0.5 ГБ), меньший blast radius, чистые границы
ресурсов под будущий API.

**Ограничение:** основной файл остаётся `db/unified_trade_data.duckdb` — торговые дашборды
Superset не ломаются; переподключить нужно только датасеты вкладки физобъёмов.

## Целевая раскладка файлов

**`db/unified_trade_data.duckdb`** (имя без изменений — торговля):
`unified_trade_data`, `unified_trade_data_enriched_base` + view `unified_trade_data_enriched`,
`coverage_matrix`, `trade_mom_kpi`, `country_reference`, `tnved_reference`, `hs4_reference`.

**`db/fizob.duckdb`** (новый — физобъёмы):
`fizob_index`, `fizob_index_v`, `fizob_enriched`, `country_reference`, `tnved_reference`
(дубль ~15 МБ — нужны для названий в `fizob_enriched`; `hs4_reference` не нужен).

Каждый файл самодостаточен — можно возить и подключать отдельно.

## Изменения по файлам

### `src/pipelines/merge_pipeline.py`
- `resolve_merge_paths`: добавить `fizob_db_path` (дефолт `db/fizob.duckdb`) + CLI `--fizob-db-path`.
- `save_fizob_index(rows, fizob_db_path)` — писать в fizob-БД, а не в основную.
- `create_reference_tables` разбить на два вызова:
  - trade-БД: справочники + enriched(base+view) + coverage + mom_kpi;
  - fizob-БД (если fizob включён): `country_reference` + `tnved_reference` + `fizob_enriched`.
- Порядок в `run_merge_pipeline`: `save_to_duckdb(trade)` → trade-марты; затем
  `save_fizob_index(fizob_db)` → fizob-марты.

### `src/core/reference_tables.py`
- Вынести билдеры справочников (`tnved`/`country`/`hs4`) в переиспользуемые хелперы.
- Разделить `save_reference_tables` на:
  - `save_trade_reference_tables` — refs + enriched + coverage + mom_kpi;
  - `save_fizob_reference_tables` — `country_reference` + `tnved_reference` + `fizob_enriched`.
- `coverage_matrix`/`trade_mom_kpi` уже читают только `unified_trade_data`(+`country_reference`) —
  остаются в trade-БД без изменений.

### `src/orchestration/checks.py`
- Торговые проверки → по trade-БД (как сейчас). `require_fizob`-проверки
  (`fizob_index`, `fizob_index_v`) → по fizob-БД. Добавить `fizob_db_path`
  (или отдельную `run_fizob_quality_checks`).

### `src/orchestration/flows.py`
- Прокинуть `fizob_db_path`; гнать trade-quality по trade-БД и fizob-quality по fizob-БД;
  в манифест писать оба артефакта.
- `fizob_queries.R` по-прежнему **читает** trade-БД
  (`--db-path db/unified_trade_data.duckdb`), чтобы посчитать индекс из фактов —
  здесь ничего не меняется; меняется только куда merge кладёт результат.

### `scripts/golden_snapshot.py`
- `aux_row_counts[fizob_index]` читать из fizob-БД (иначе снимок «потеряет» fizob).

### `scripts/slice_duckdb_by_period.py`
- Ручная утилита (не в пайплайне): вырезает срез за диапазон лет в отдельный лёгкий файл.
- **Решение:** сделать срез только торговым — убрать ветку копирования fizob (в trade-БД
  физобъёмов больше нет). Понадобится fizob-срез — зеркалить логику против `fizob.duckdb`
  отдельно.

## Superset — минимальное касание

- **Торговые датасеты** (`unified_trade_data_enriched`, `coverage_matrix`, `trade_mom_kpi`) —
  не трогаем: тот же файл и имена.
- **Вкладка физобъёмов** (`fizob_enriched`, `fizob_index_v`) — создать новое DuckDB-подключение
  к `db/fizob.duckdb` и пере-указать эти датасеты на него. Разово.

## Как это готовит будущий API

Тонкий auth-сервис (напр. FastAPI, read-only коннекты) делает
`ATTACH 'unified_trade_data.duckdb'` и `ATTACH 'fizob.duckdb'`, роутит `/trade/*` и
`/fizob/*`, при желании — разные скоупы токена на домены. DuckDB — встраиваемая БД без
сетевого слоя/авторизации, поэтому доступ по токену и из Excel обеспечивает именно сервис
перед файлами (REST/OData для Power Query, либо Postgres-wire фронт для «живого» SQL из
Excel). Деление на два файла даёт чистые границы ресурсов бесплатно.

## Порядок раскатки

1. Реализовать двух-файловую сборку, прогнать локально.
2. Сверить: trade-БД идентична текущей по торговым объектам (golden по торговле совпадает),
   fizob-БД содержит `fizob_index`/`fizob_index_v` + `fizob_enriched`.
3. В Superset добавить подключение к `fizob.duckdb`, пере-указать 2 fizob-датасета.
   Торговые дашборды не трогаются.
4. Заливать два файла на VPS.

## Решения (2026-07-30)

1. Имя файла: **`db/fizob.duckdb`** — ок.
2. `slice_duckdb_by_period`: **срез только торговый** (fizob-ветку убрать).
3. Отсутствие fizob-БД: quality-gate **предупреждает**, не падает жёстко.

## Примечания

- Работа чисто структурная — семантика и значения данных не меняются; golden по торговле
  должен совпасть.
- Затрагивает ~6 файлов, каждое изменение локальное.
- Справочники дублируются в обеих БД (~15 МБ) ради самодостаточности — сознательный размен
  против `ATTACH`-зависимости между файлами.
