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
  the Superset cabinet at `http://217.26.28.186:8088/apikey/`.
- Base URL defaults to the pilot VPS `http://217.26.28.186:8090`. Override with
  `MGIMO_API_BASE` (e.g. `http://localhost:8000` for a local dev server; its dev
  token is `dev-token`).
- If `query.py meta` returns your plan and data version, auth works.

## The helper

Run from the repo root:

```bash
python .claude/skills/trade-data/query.py <subcommand> [flags]
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

## How to answer well

- **Filter and aggregate first.** Never fetch raw millions of rows. For "China oil
  imports in 2024 by month", group by `tnved2,period` and filter `--strana CN --napr
  im --tnved2 27 --period-from 2024-01 --period-to 2024-12`.
- Keep `--limit` modest; the endpoint paginates and `meta.has_more`/`next_offset`
  tells you if more exist.
- Read `meta` in the response (rows, table, has_more) and the units above before
  stating a figure. State the unit (USD / kg / index) and the period.
- Values are USD and kg in absolute units; format large numbers readably (bln/mln).
- **Growth / "what grew most" questions** compare two periods: run the same
  aggregation twice (e.g. `--period-from 2025-01 --period-to 2025-07` and the 2026
  window), join by the grouping key locally, and rank by the difference. Report both
  the absolute change and the percent, and note a small base when a percent looks huge.
  For price-cleaned growth use `fizob` (`idx`) instead of `stoim`.

## Examples

```bash
# Russia's imports from China, oil & fuels (HS 27), monthly, 2024, with names
python .claude/skills/trade-data/query.py trade \
  --strana CN --napr im --tnved2 27 --group-by tnved2,period \
  --include tnved2_name --period-from 2024-01 --period-to 2024-12

# Top import product groups from Turkey in 2025 (value), names included
python .claude/skills/trade-data/query.py trade \
  --strana TR --napr im --group-by tnved2 --include tnved2_name \
  --period-from 2025-01 --order-by stoim --desc --limit 15

# Physical-volume index: China imports, HS4 level, from 2024
python .claude/skills/trade-data/query.py fizob \
  --strana CN --napr im --tn-level 4 --period-from 2024-01

# Data freshness and your usage
python .claude/skills/trade-data/query.py meta

# OData escape hatch (Excel-style filter)
python .claude/skills/trade-data/query.py get \
  "/odata/fizob?\$filter=STRANA eq 'CN' and tn_level eq 4&\$top=20"
```

## Guardrails

- Read-only: this only issues GETs. Never attempt writes.
- Do not print or echo the token. If auth fails, tell the user to set
  `MGIMO_API_TOKEN` (see above), do not work around it.
- Ground every number in an actual response; if the API is unreachable, say so
  rather than estimating.
