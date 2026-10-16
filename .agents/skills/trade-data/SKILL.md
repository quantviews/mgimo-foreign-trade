---
name: trade-data
description: Answer questions about Russia's foreign-trade statistics (imports/exports by country, HS/TNVED product, month; values in USD, weight in kg, and physical-volume indices) by querying the project's read-only data API. Use whenever the user asks what/how much a country traded with Russia, product-group dynamics, oil vs non-oil, physical-volume (fizob) index, or wants a figure grounded in the DuckDB serving layer rather than guessed.
---

# Trade data Q&A over the API

Answer data questions by calling the project API with `query.py` (stdlib only, no
install). Compose a filtered/aggregated request, run it, read the JSON, answer with
the numbers. Do not invent figures; if a call fails, report the error.

## Token and base URL

- Token is read from `MGIMO_API_TOKEN` (env var, else a line in the repo-root `.env`).
  Never put the token in this file, in commits, or in printed output. Get a key from
  the Superset cabinet at `https://nts.mgimo.ru/superset/apikey/`.
- Base URL defaults to the pilot VPS `https://nts.mgimo.ru/api`. Override with
  `MGIMO_API_BASE` (e.g. `http://localhost:8000` for a local dev server; its dev
  token is `dev-token`).
- If `query.py meta` returns your plan and data version, auth works.

## The helper

Run from the repo root:

```bash
python .agents/skills/trade-data/query.py <subcommand> [flags]
```

- `trade`   aggregated trade (`/v1/trade`)
- `fizob`   physical-volume index (`/v1/fizob`)
- `meta`    your usage/plan and the data version
- `reference`  code dictionaries: `countries`, or `tnved --level N` (2/4/6/8/10)
- `get PATH`  arbitrary GET, for OData (`/odata/trade`, `/odata/fizob`)

Sort descending with `--order-by <col> --desc` (not a leading `-`, which argparse
reads as a flag). `--order-by` must name a grouped dimension or a selected metric.

Filters that accept lists (`--strana`, `--napr`, `--tnved2`, `--tn-level`, ...) are
repeatable: pass the flag again per value.

## Domain rules (read before composing a query)

- **Direction `NAPR`**: `ИМ` = import **into** Russia, `ЭК` = export **from** Russia.
  On `--napr` you may pass `im`/`ex` (aliased). "Russia's imports of X" -> `im`.
- **Country `STRANA`**: ISO2 code, e.g. `CN` China, `IN` India, `TR` Turkey, `DE`
  Germany. Ask for the code if a country is ambiguous, or check `reference country`.
- **Product `TNVED`**: HS/TNVED codes as strings, leading zeros kept. Filter or group
  by level: `tnved2` (2-digit group), `tnved4`, `tnved6`, `tnved` (full). Product
  names come via `--include tnved2_name` / `tnved4_name` / `tnved_name` (each needs
  its code in `--group-by`).
- **Period**: months as `YYYY-MM` (or `YYYY-MM-DD`), first of month. Use
  `--period-from` / `--period-to`.
- **Metrics**: `stoim` = value in **USD** (not thousands), `netto` = net weight in
  **kg**, `kol` = quantity in the supplementary unit. Default is `stoim,netto`. `kol`
  is only meaningful with `edizm` in `--group-by` (different units per product cannot
  be summed); for cross-product volume use `netto` (kg) or `stoim` (USD), or the
  fizob index.
- **`TYPE`**: `fact` = observed, `pred` = nowcast forecast (filled only where a
  source lags). Default includes both. To exclude the forecast, add `--type fact`.
- **`SOURCE`**: `national` (CN/IN/TR customs), `comtrade` (mirror), `nowcast`.
- **Physical volume**: for "real volume dynamics cleaned of prices", use `fizob`, not
  raw `stoim`. `fizob` rows carry `idx` (the index, the headline number), at
  `tn_level` `0` (country total), `2`, `4`, `6`; `tn_name` is the product name for
  levels 2/4. `fizob`/`fizob_bp` are working values behind `idx`.

## Country comparison through MCP

When MCP is connected, use `compare_trade` FIRST for country comparisons, instead of exploratory trade/export calls. Example: `strana=["IN","CN"], napr="ex", tnved4=["2709"], months=12`. It resolves the latest month, intersects factual months and computes exact differences/percentages against the first country. Present `answer_ru` or a concise faithful paraphrase; do not recompute from rounded figures or add unsolicited year-on-year analysis. No file is created. If using only query.py (which has no compare subcommand), follow the monthly-aggregate rules below.

For country totals or one HS2/4/6 code, compare_trade also returns physical_volume by default. Preserve its method: change between common endpoint months of a 12-month-smoothed index, NOT growth of period totals or necessarily year-on-year growth. Start=100 is rebased independently per country; differences in growth are percentage points. Never sum indices. Aggregation of different physical units in country totals is an intentional feature of the project methodology for heterogeneous product baskets, not by itself an error or limitation. Do not confuse it with summing raw KOL across units. Preserve the distinctions between index dynamics, tonnage and exact price/volume decomposition, as well as explanations of smoothing and restored quantities. If physical_volume.available=false, state the reason without inventing an index or shifting the period.

## Comparable periods and analyst-facing answers

- For country comparisons, fetch monthly aggregates by country, period, type and source (not raw records). Compare the intersection of months with factual observations for every requested country within the requested interval. Missing countries or months are not zero. If no common months exist, say a factual comparison is unavailable.
- For "latest 12 months", anchor the requested window to meta.period_max. If only 11 common fact months exist, label the comparison as 11 months and list the excluded months; do not silently shift the window backwards. For year-on-year comparisons, align the same calendar months in both years before summing. Enumerate gaps rather than presenting a discontinuous set as a continuous period.
- Show absolute differences and percentages with an explicit denominator (comparison baseline). Do not compute percentages against a zero baseline; flag small bases. Verify comparable direction, product scope and units. Matching months alone does not guarantee equal source/product coverage. A null metric is not zero and cannot establish comparable coverage for that metric.
- Keep fact and pred separate. Do not add estimates to factual totals or use them to fill a comparison without explicitly labeling a separate scenario. A zero-valued forecast row must not extend the displayed period of an estimate; retain it in the export and explain its presence when relevant. Do not infer a whole month is forecast from the mere existence of one pred row. Check overlap at the detailed product-code level before claiming double counting.
- An export's summary totals may cover different periods by country: never compare those totals directly. Use the bounded monthly data to calculate matched-period totals. Never call has_more/truncated results complete. Respect export quotas and Retry-After; do not split requests to evade them.
- Lead with 1-2 substantive findings, then a compact table, then necessary qualifications and the download link if available. Use country names and plain labels (стоимость, масса, факт, модельная оценка), not internal API field names. Keep technical filters out of the main narrative.
- In Russian answers use space-separated thousands and a decimal comma: 112 756,9; 60,02 млрд долл.; 8,4%. Calculate with unrounded values, round only for display, and name units. Mass is not a price-adjusted physical-volume index. Do not invent explanations for observed differences.


## Explain dynamics from aggregate to subcategories

For year-on-year drivers of one country and one HS2/HS4 code, call MCP explain_trade FIRST: strana="CN", napr="im", code="8703", months=12. It returns calculated parent totals, shares, contribution_pp and unmatched_codes/residual. Preserve these values and missing-data caveats; do not redo the arithmetic. With query.py alone use the complete two-period workflow above.


For analytical questions about changes, drivers, structure or "why", start with the parent total and its comparable-period change, then drill down ONE level (HS2 -> HS4, HS4 -> HS6). Do not add this investigation to a simple file request. For 8703 use tnved6, never assume every subcategory is an electric vehicle.

1. Establish the same calendar months, direction, country, source and fact status in current and prior-year periods. First verify parent monthly coverage. Retain the chosen window; missing months are not zeros. National statistics, Comtrade source estimates and project nowcast remain distinct.
2. Fetch complete child aggregates for BOTH periods, retaining the parent filter. Usually two trade calls with group_by=tnved6,type,source (plus edizm,edizm_iso for quantity), metrics=stoim,netto,kol and include=tnved6_name suffice after coverage checks. Aggregate over each entire matched window; do not fetch 24 months times all child codes unless necessary. Check has_more/truncation; never use a top-N subset as the denominator. Reconcile the sum of children with the independently obtained parent totals for both periods. Show a residual as unallocated, not zero; unresolved coverage means decomposition is incomplete.
3. For value V_i of child i and parent T, compute prior and current shares 100*V_i/T, absolute change d_i=V_i(current)-V_i(prior), child's growth 100*d_i/V_i(prior), and contribution to PARENT growth in percentage points 100*d_i/T(prior). Contributions reconcile to parent percentage growth. Do not confuse a child's growth %, its share %, and contribution pp. Avoid d_i/sum(d_i) as the default contribution: offsetting movements or a near-zero net change make it misleading. No percentages with zero/unknown denominator. An absent child in one period is unknown unless zero coverage is independently confirmed, not an automatic zero.
4. Rank by absolute change magnitude, show 3-5 leading positive/negative contributors, their names and codes, previous/current shares, absolute changes and contributions pp. Account for other categories as a residual ONLY from complete reconciled data. Lead with a substantive finding (broad-based change or concentrated in specific categories). Keep tables compact. Retain full precision for calculations; Russian display uses spaced thousands, decimal commas, mass in thousand tonnes.
5. Quantity is additive only within matching units and complete coverage. Do not convert partial known quantities into a total. For fizob use project methodology: heterogeneous physical units can be aggregated by the index methodology, but child indices cannot simply be summed or treated as additive contributions. Monetary decomposition is not a decomposition of real-volume growth. Structural contributions explain WHICH categories changed, not WHY tariffs, prices or consumer behavior caused it without other evidence.

## How to answer well

- **Filter and aggregate first.** Never fetch raw millions of rows. For "China oil
  imports in 2024 by month", group by `tnved2,period` and filter `--strana CN --napr
  im --tnved2 27 --period-from 2024-01 --period-to 2024-12`.
- Keep `--limit` modest; the endpoint paginates and `meta.has_more`/`next_offset`
  tells you if more exist.
- Read `meta` in the response (rows, table, has_more) and the units above before
  stating a figure. State the unit (USD / kg / index) and the period. Every `trade`
  response also carries `meta.period_max` (the latest available month), so you do
  not need a separate `meta` call just to bound "latest" or "this year".
- Values are USD and kg in absolute units; format large numbers readably (bln/mln).

## Fast answers: keep it to 1-2 calls

The API computes SUM/GROUP BY server-side in tens of milliseconds, so let it do the
work: one aggregation for a plain question, two for a comparison. Do not pull raw
rows and slice locally, size `--limit` to the answer (e.g. 15, not thousands), and
include monthly/type/source breakdowns when needed to establish comparability.
For comparisons keep fact and pred separate, even if the user did not ask explicitly. Following
these recipes keeps a question to a couple of calls instead of an open exploration.

- **"How much did <country> import/export of <X>"**: one `trade` call, group by
  `period` (or `year`), filter country/napr/product, metric `stoim`.
- **"Top products / partners by value"**: one `trade` call, group by `tnved2` (or
  `strana`), `--include tnved2_name`, `--order-by stoim --desc --limit 15`.
- **"What grew / fell most (value)"**: group by `tnved2,period,type` over the bounded range,
  align factual calendar months across years, then calculate changes and rank; or, for two specific months, two calls grouped by `tnved2`.
  Give the absolute change and the percent, and flag a small base.
- **"Compare years / totals across years"**: use monthly factual aggregates to align calendar months
  across years; aggregate to years only after verifying matching coverage.
- **"...in physical terms / tonnage"**: same as growth but metric `netto` (kg), not
  `stoim`. Weight is additive across countries, so group by `tnved2` directly.
- **"Real / price-cleaned volume dynamics of <country>'s <product>"**: use `fizob`
  (`idx`) at the matching `tn_level`; `idx` is already a volume index. Do not sum
  `idx` across products (not additive) - compare one series over time.

When you post-process a response with your own Python on Windows, open files with
`encoding="utf-8"` (both read and write) - the JSON has Cyrillic, and the default
codepage raises UnicodeDecodeError, forcing a slow retry. Simplest: pipe the two
calls to files and load them with `json.load(open(path, encoding="utf-8"))`.

## Sanity-check before you conclude

- **Split price from volume.** A value change is price times quantity. Before
  calling a move "mostly price" (or "mostly volume"), pull `netto` too and split it:
  value ratio = ($/kg ratio) x (kg ratio). Report both parts. Do not attribute a
  surge to price without checking that weight did not also move.
- **Cross-check `netto` against `fizob`.** For a physical-volume claim, compare raw
  weight (`netto`, kg) with the `fizob` index. When they disagree (weight doubles
  but `idx` is flat, say), do not pick one: flag the contradiction and treat the
  series as suspect. `fizob` can be distorted by an anomalous base period (a spike
  in the base month inflates `fizob_bp`), so a flat or tiny `idx` next to a moving
  `netto` is a red flag, not a finding.
- **Implausible unit values are data, not news.** Compute `$/kg` and reality-check
  it (e.g. crude oil far above the world price; Russian grades trade at a discount).
  A large swing in a fact month, especially in India's national data (MEIDB has
  skeleton months, lags and valuation quirks), is more likely a source artifact than
  a real event: say so and suggest re-checking after the data is revised.
- **Lean on relative comparisons, not absolute levels.** Gaps, ratios, ranks, shares
  and month-over-month or year-over-year change are verifiable inside the data; an
  absolute level (a price in $/bbl, a value in USD) usually cannot be checked against
  the outside world here. Prefer "India pays a widening premium to China" over "the
  price was $123/bbl". When an absolute figure carries the point, say it cannot be
  externally validated and could be a level shift in the source.

## Examples

```bash
# Russia's imports from China, oil & fuels (HS 27), monthly, 2024, with names
python .agents/skills/trade-data/query.py trade \
  --strana CN --napr im --tnved2 27 --group-by tnved2,period \
  --include tnved2_name --period-from 2024-01 --period-to 2024-12

# Top import product groups from Turkey in 2025 (value), names included
python .agents/skills/trade-data/query.py trade \
  --strana TR --napr im --group-by tnved2 --include tnved2_name \
  --period-from 2025-01 --order-by stoim --desc --limit 15

# Physical-volume index: China imports, HS4 level, from 2024
python .agents/skills/trade-data/query.py fizob \
  --strana CN --napr im --tn-level 4 --period-from 2024-01

# Data freshness and your usage
python .agents/skills/trade-data/query.py meta

# OData escape hatch (Excel-style filter)
python .agents/skills/trade-data/query.py get \
  "/odata/fizob?\$filter=STRANA eq 'CN' and tn_level eq 4&\$top=20"
```

## Guardrails

- Read-only: this only issues GETs. Never attempt writes.
- Do not print or echo the token. If auth fails, tell the user to set
  `MGIMO_API_TOKEN` (see above), do not work around it.
- Ground every number in an actual response; if the API is unreachable, say so
  rather than estimating.

Use canonical Russian database product names followed by codes in parentheses. explain_trade returns name/name_source/name_status. Use the Russian name as the reference; concise faithful labels with exact codes are allowed. If name is missing, keep the code and flag the missing label. Hybrid HS6 names and their zero-padded aggregates are fixed in the persisted Russian reference; the MCP English fallback has been removed.


## Аналитический ответ: суть и драйверы

Начинай с результата для аналитика: изменение агрегата, величина и сопоставимый период. В первом абзаце назови также 1-2 главных драйвера, до таблицы. Источник достаточно кратко указать в конце. Для одного короткого ограничения используй примечание, без отдельного большого раздела «Оговорки». Пиши содержательно: «Динамика стоимости включает изменение количества и цен», без противопоставления «это не физические объемы». Затем покажи 3-5 главных подкатегорий с названиями и точными кодами, их доли и вклад в динамику. Объясни, насколько рост сосредоточен в отдельных категориях и как изменилась структура. Лидеров определяй по данным: лидер по вкладу не обязательно лидер по доле или темпу роста. Причины вне данных (спрос, цены, политика) обозначай как гипотезы, только если они полезны вопросу и имеют основание; не придумывай их.

Пиши прямо: «Источник: таможенная статистика Китая». Не перечисляй исключенные источники («не Comtrade», «не nowcast») и не строй ответ на оборотах «это не X, а Y», если пользователь не просит разъяснить различия. Внутренние source/type, флаги и формулы не пересказывай как чек-лист. Проверки делай всегда, публикуй только выводы из них, нужные для понимания ответа.

Существенное ограничение ставь рядом с затронутым показателем: отсутствующий месяц, неполное количество, оценка вместо наблюдения, несопоставимость периодов. Если оно делает основной вывод невозможным, сообщи об этом сразу. Иначе краткую справку об источнике, периоде и существенном остатке разложения оставь в конце. Не добавляй общие оговорки о пропусках и оценках без конкретного основания в выбранном срезе. Малый нераспределенный остаток укажи соразмерно его влиянию, без длинного методологического вступления. Не скрывай остаток и не считай пропуски нулями.

Названия в чате можно сократить для читаемости, сохранив смысл русского справочника и точный код в скобках. В Excel остаются исходные названия. Факт, оценки источника Comtrade и nowcast проекта учитывай раздельно; поясняй различие там, где оно влияет на рассматриваемый показатель. Сначала содержание ответа, затем краткая справка и ссылка на файл, если запрошен.


## Топ товарных групп

Для рейтинга групп по стоимости используй MCP `rank_trade` первым: `rank_trade(strana="TR", napr="im", top_n=10)` для импорта из Турции в этом году. Без дат инструмент выбирает январь - последний месяц года из актуальности базы. Для другого периода передай обе даты. Инструмент работает по главам ТН ВЭД-2 и факту, возвращает названия, доли и общий объем по ВСЕМ группам; в `data` только запрошенные лидеры.

`ranking.complete_universe=true` и `shown_groups < total_groups` означают выбранный топ из полного списка, а не усечение. Используй готовые `total_stoim`, `share_pct`, `top_share_pct`, `other_groups_stoim`. Назови фактические месяцы и пропуски из `fact_months` / `missing_months`. Полный знаменатель не гарантирует полноты исходной статистики. Excel для рейтинга не нужен.

Если MCP недоступен, через `query.py trade` получи все агрегаты `tnved2` с `type=fact`, названиями, сортировкой `-stoim` и достаточным limit; проверь `meta.has_more`. Считай знаменатель только по полному списку, затем выбери топ. В обычном `trade` флаги `has_more` / `_truncated` остаются признаком неполного ответа: нельзя просто игнорировать их или считать доли от суммы первых N строк. Проверь месяцы отдельным агрегатом. Не превращай данные за часть года в годовой итог.

## Область предупреждений о качестве

Область предупреждений о качестве: связывай каждый флаг только с проверяемым показателем. Предупреждение о массе относится к массе и производным от нее (цена за кг, динамика массы); предупреждение о количестве или единице относится к количеству и производным от него. Низкая масса на транспортное средство сама по себе не доказывает ошибку стоимости. Стоимость, ее динамику и доли по стоимости ставь под сомнение только при отдельном свидетельстве проблемы стоимости или покрытия. Для топа по стоимости не добавляй оговорку о массе, если масса не используется в выводах. Не скрывай соответствующие флаги при ответах о физических показателях. Если предупреждение неоднозначно, не расширяй его смысл на другие показатели.

## Неполные периоды и нулевые строки

Неполные периоды и нулевые строки: различай запрошенное окно, месяцы имеющихся фактических записей и nowcast. Если фактического месяца нет, пиши «Полный фактический итог за запрошенный период не установлен; в доступных записях отражено ...». Не характеризуй масштаб торговли как малый или редкий по неполному срезу. Количество или массу при пропусках называй только известной частью. Нулевая стоимость в существующей строке означает запись со значением 0, а не отсутствие наблюдения; не объясняй ее округлением или отсутствием торговли без подтверждения источника. NULL не равен нулю. Национальный источник и TYPE=fact не доказывают непосредственно наблюдаемую природу физических показателей.
Для Excel одного товара HS2/HS4 и одной страны с аналитическим разбором сначала вызови explain_trade за запрошенное окно (не более 12 месяцев), затем export_trade за тот же период. available=false или missing_months запрещает полный рост год к году, доли и вклады в изменение за это окно. Не сдвигай период молча. Выгрузка доступных записей допустима. complete/has_more описывают передачу строк API, а не полноту исходной статистики. Nowcast не заполняет пробелы факта в сопоставлении; оценки физических показателей Comtrade сохраняют источник и не являются nowcast проекта.
