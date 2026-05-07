# Инструкция по созданию страновых collectors/processors

Этот документ описывает, как добавлять новый страновой источник данных так, чтобы он сразу был совместим с Python ETL, контрактными тестами и финальным `merge_processed_data` pipeline.

## Термины

В проекте полезно разделять две роли:

*   **Collector** скачивает или извлекает сырые данные и складывает их в `data_raw/<country>/`.
*   **Processor** приводит сырые данные страны к единому контракту и сохраняет parquet в `data_processed/<iso>_full.parquet`.

Финальный merge pipeline работает только с результатом processor-а. Поэтому главный стабильный интерфейс — это не HTML/CSV/API конкретного источника, а выходной DataFrame processor-а.

## Где размещать код

Рекомендуемая структура:

```text
src/collectors/<country>_collector.py
src/collectors/<country>_processor.py
data_raw/<country>/
data_processed/<iso>_full.parquet
docs/<country>-collector-docs.md
docs/<country>-processor-docs.md
```

Примеры:

*   `src/collectors/china_processor.py` -> `data_processed/cn_full.parquet`
*   `src/collectors/india_processor.py` -> `data_processed/in_full.parquet`
*   `src/collectors/turkey_processor.py` -> `data_processed/tr_full.parquet`

## Стандартные входы processor-а

Новые processor-ы должны принимать стандартные пути через общий контрактный слой:

```python
from core.country_processor_contract import CountryProcessorInput

processor_input = CountryProcessorInput.from_paths(
    raw_data_dir,
    output_file,
    country_code="XX",
    edizm_file=edizm_file,
)
```

Поля:

*   `raw_data_dir` — папка с сырыми файлами страны.
*   `output_file` — целевой parquet-файл.
*   `country_code` — ISO2 код страны в верхнем регистре.
*   `edizm_file` — обычно `metadata/edizm.csv`.
*   `metadata_dir` — выводится из `edizm_file.parent`, если явно не задан.

Старые публичные функции вида `process_and_merge_<country>_data(raw_data_dir, output_file, edizm_file)` можно сохранять для совместимости, но внутри они должны приводить параметры к `CountryProcessorInput`.

## Стандартный выходной DataFrame

Processor обязан вернуть и сохранить DataFrame с колонками:

```text
NAPR, PERIOD, STRANA, TNVED, EDIZM, EDIZM_ISO,
STOIM, NETTO, KOL, TNVED4, TNVED6, TNVED2
```

Семантика:

*   `NAPR` — только `ИМ` или `ЭК`.
*   `PERIOD` — `datetime64`, дата первого дня месяца.
*   `STRANA` — ISO2 код страны, uppercase.
*   `TNVED` — строковый код товара.
*   `TNVED2`, `TNVED4`, `TNVED6` — префиксы `TNVED`.
*   `STOIM` — стоимость в тысячах USD.
*   `NETTO` — вес нетто в кг.
*   `KOL` — количество в дополнительной единице.
*   `EDIZM` — каноническое название единицы измерения.
*   `EDIZM_ISO` — ISO-код единицы измерения.

## Единые post-processing шаги

Не нужно вручную повторять сортировку, типизацию и генерацию обязательных колонок. Используйте:

```python
from core.country_processor_contract import finalize_country_output, save_country_output

final_df = finalize_country_output(raw_df, country_code="XX")
save_country_output(final_df, processor_input.output_file, logger=logger)
```

`finalize_country_output()` делает:

*   добавляет отсутствующие обязательные колонки;
*   нормализует `NAPR` (`IMPORT` -> `ИМ`, `EXPORT` -> `ЭК`, `1` -> `ИМ`, `2` -> `ЭК`);
*   приводит `PERIOD` к datetime и нормализует время;
*   проставляет `STRANA`;
*   приводит `STOIM`, `NETTO`, `KOL` к numeric;
*   пересчитывает `TNVED2`, `TNVED4`, `TNVED6`;
*   выставляет порядок колонок;
*   удаляет дубли;
*   сортирует результат.

## Единый слой TNVED и EDIZM

Нельзя заводить новый локальный словарь единиц измерения внутри processor-а. Все общие правила лежат в:

```text
src/core/normalization_rules.py
```

Для единиц измерения используйте:

```python
from core.edizm import load_common_edizm_mapping, resolve_edizm_records

project_root = processor_input.metadata_dir.parent
common_edizm_map = load_common_edizm_mapping(project_root)
records = resolve_edizm_records(raw_units, common_edizm_map)

df["EDIZM_ISO"] = records.map(lambda r: r.get("KOD") if isinstance(r, dict) else None)
df["EDIZM"] = records.map(lambda r: r.get("NAME") if isinstance(r, dict) else None)
```

Если у нового источника есть нестандартное обозначение единицы, добавляйте alias в `COUNTRY_UNIT_ALIAS_RECORDS` в `src/core/normalization_rules.py`, а не в страновой processor.

Для TNVED используйте:

```python
from core.tnved import add_tnved_columns

df = add_tnved_columns(df)
```

Если processor использует `finalize_country_output()`, отдельный вызов `add_tnved_columns()` чаще всего не нужен: контрактный post-processing пересчитает `TNVED2/4/6`.

## Минимальный шаблон processor-а

```python
from pathlib import Path
import logging
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.country_processor_contract import (
    CountryProcessorInput,
    finalize_country_output,
    save_country_output,
)
from core.edizm import load_common_edizm_mapping, resolve_edizm_records


logger = logging.getLogger(__name__)


def process_and_merge_example_data(raw_data_dir: Path, output_file: Path, edizm_file: Path):
    processor_input = CountryProcessorInput.from_paths(
        raw_data_dir,
        output_file,
        country_code="XX",
        edizm_file=edizm_file,
    )

    project_root = processor_input.metadata_dir.parent
    common_edizm_map = load_common_edizm_mapping(project_root)

    frames = []
    for file_path in sorted(processor_input.raw_data_dir.glob("*.csv")):
        df = pd.read_csv(file_path, dtype={"TNVED": str})

        df["PERIOD"] = pd.to_datetime(df["year"].astype(str) + "-" + df["month"].astype(str) + "-01")
        df["STRANA"] = processor_input.country_code
        df["NAPR"] = df["direction"]
        df["STOIM"] = pd.to_numeric(df["value"], errors="coerce")
        df["NETTO"] = pd.to_numeric(df["netto"], errors="coerce")
        df["KOL"] = pd.to_numeric(df["quantity"], errors="coerce")

        records = resolve_edizm_records(df["unit"], common_edizm_map)
        df["EDIZM_ISO"] = records.map(lambda r: r.get("KOD") if isinstance(r, dict) else None)
        df["EDIZM"] = records.map(lambda r: r.get("NAME") if isinstance(r, dict) else None)

        frames.append(df)

    if not frames:
        logger.error("No input files found.")
        return

    final_df = finalize_country_output(
        pd.concat(frames, ignore_index=True),
        country_code=processor_input.country_code,
    )
    save_country_output(final_df, processor_input.output_file, logger=logger)
```

## Контрактные тесты

Для каждой новой страны добавьте тест в `tests/test_processor_contracts.py`.

Минимум:

*   создать маленький fixture-файл в `tmp_path`;
*   запустить processor;
*   прочитать parquet;
*   вызвать `assert_output_contract(df, expected_strana="XX")`;
*   отдельно проверить страновые правила: направление торговли, масштаб `STOIM`, период, специфичные единицы.

Пример:

```python
def test_example_processor_contract(tmp_path):
    (tmp_path / "example.csv").write_text("...", encoding="utf-8")
    output = tmp_path / "xx_test.parquet"

    example_processor.process_and_merge_example_data(
        tmp_path,
        output,
        tmp_path / "edizm_missing.csv",
    )

    df = pd.read_parquet(output)
    assert_output_contract(df, expected_strana="XX")
```

## Checklist перед merge

Перед тем как считать новый collector/processor готовым:

*   Нет локальных словарей единиц измерения в processor-е.
*   Все новые unit aliases добавлены в `src/core/normalization_rules.py`.
*   Processor использует `CountryProcessorInput`.
*   Processor завершает работу через `finalize_country_output()`.
*   Processor сохраняет результат через `save_country_output()`.
*   Есть контрактный тест в `tests/test_processor_contracts.py`.
*   `pytest -q` проходит.
*   Документация страны добавлена в `docs/<country>-collector-docs.md` или `docs/<country>-processor-docs.md`.
