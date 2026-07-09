# Генерирует parquet-снимки для bulletin_comparison.qmd.
# Запуск из корня репозитория: Rscript site/bulletin_data_prep.R
# DuckDB: MGIMO_DUCKDB_PATH или db/unified_trade_data.duckdb в корне репо.

suppressPackageStartupMessages({
  library(tidyverse)
  library(duckdb)
  library(arrow)
  library(forecast)
  library(jsonlite)
})

`%||%` <- function(x, y) if (!is.null(x) && nzchar(x)) x else y

repo_root <- Sys.getenv("GITHUB_WORKSPACE") %||%
  normalizePath(
    if (basename(getwd()) == "site") ".." else getwd(),
    winslash = "/",
    mustWork = TRUE
  )

db_path <- Sys.getenv("MGIMO_DUCKDB_PATH") %||%
  file.path(repo_root, "db", "unified_trade_data.duckdb")

data_dir <- file.path(repo_root, "site", "data")
dir.create(data_dir, recursive = TRUE, showWarnings = FALSE)

if (!file.exists(db_path)) {
  stop(
    "DuckDB not found: ", db_path,
    "\nSet MGIMO_DUCKDB_PATH or place db/unified_trade_data.duckdb in the repo root."
  )
}

message("bulletin_data_prep: db=", db_path)
message("bulletin_data_prep: out=", data_dir)

con <- dbConnect(duckdb::duckdb(), db_path, read_only = TRUE)
on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)

replace_outlier <- function(x, frequency = 12) {
  as.numeric(tsclean(ts(x, frequency = frequency)))
}

oil_tnved4 <- c("2709", "2710", "2712", "2713", "2714", "2715")
gas_tnved4 <- "2711"

strana_oilgas_ru <- c(
  CN = "Китай",
  IN = "Индия",
  TR = "Турция",
  JP = "Япония",
  HU = "Венгрия",
  SK = "Словакия"
)

tab_stoim <- dbGetQuery(
  con,
  "SELECT STRANA, NAPR, PERIOD, STOIM FROM unified_trade_data"
) %>%
  reframe(STOIM = sum(STOIM, na.rm = TRUE), .by = c(STRANA, NAPR, PERIOD))

strana_out <- dbGetQuery(
  con,
  "SELECT STRANA, NAPR, PERIOD, tn_level, tn_code, fizob, fizob_bp FROM fizob_index"
) %>%
  filter(tn_level == 0) %>%
  filter(STRANA %in% STRANA[fizob > 10000]) %>%
  pull(STRANA) %>%
  unique()

bulletin_fo <- dbGetQuery(
  con,
  "SELECT STRANA, NAPR, PERIOD, tn_level, tn_code, fizob, fizob_bp FROM fizob_index"
) %>%
  filter(STRANA %in% setdiff(STRANA, strana_out)) %>%
  filter(tn_level == 0) %>%
  left_join(tab_stoim, by = c("STRANA", "NAPR", "PERIOD")) %>%
  mutate(
    STRANA = if_else(STRANA %in% c("IN", "TR", "CN"), STRANA, "OTHER"),
    fizob = if_else(STRANA == "OTHER", fizob * STOIM, fizob)
  ) %>%
  reframe(fizob = mean(fizob, na.rm = TRUE), .by = c(STRANA, NAPR, PERIOD)) %>%
  arrange(STRANA, NAPR, PERIOD) %>%
  mutate(
    fizob = if_else(STRANA == "OTHER", replace_outlier(fizob), fizob),
    .by = c(STRANA, NAPR)
  ) %>%
  mutate(
    fizob = -1 + fizob / lag(fizob, 12),
    .by = c(STRANA, NAPR)
  ) %>%
  filter(PERIOD >= ymd("2020-01-01"))

tab_stoim_oil <- dbGetQuery(
  con,
  "SELECT STRANA, NAPR, PERIOD, STOIM, TNVED4 FROM unified_trade_data"
) %>%
  filter(NAPR == "ЭК") %>%
  mutate(
    type = if_else(
      TNVED4 %in% c(oil_tnved4, gas_tnved4),
      "Нефтегазовый",
      "Кроме нефти и газа"
    ),
    STRANA = if_else(STRANA %in% c("IN", "TR", "CN"), STRANA, "OTHER")
  ) %>%
  reframe(STOIM = sum(STOIM, na.rm = TRUE), .by = c(STRANA, NAPR, PERIOD, type)) %>%
  mutate(STOIM = STOIM / 10^9)

df_groups <- dbGetQuery(
  con,
  "SELECT STRANA, NAPR, PERIOD, STOIM, NETTO, KOL, TNVED4 FROM unified_trade_data"
) %>%
  mutate(STOIM = STOIM / 10^9) %>%
  reframe(
    across(c(STOIM, NETTO, KOL), ~ sum(.x, na.rm = TRUE)),
    .by = c(TNVED4, NAPR, PERIOD)
  ) %>%
  mutate(
    max_1 = max(PERIOD),
    min_1 = max_1 %m-% months(11),
    max_2 = min_1 %m-% months(1),
    min_2 = max_2 %m-% months(11)
  ) %>%
  mutate(
    year = case_when(
      PERIOD >= min_1 ~ "last12",
      PERIOD >= min_2 & PERIOD <= max_2 ~ "year_before",
      .default = "other"
    )
  ) %>%
  filter(year %in% c("last12", "year_before")) %>%
  reframe(
    across(c(STOIM, NETTO, KOL), ~ sum(.x, na.rm = TRUE)),
    .by = c(TNVED4, NAPR, year)
  ) %>%
  select(TNVED4, NAPR, year, STOIM, NETTO, KOL) %>%
  pivot_wider(names_from = year, values_from = c(STOIM, NETTO, KOL)) %>%
  mutate(
    STOIM_diff = STOIM_last12 - STOIM_year_before,
    STOIM_gr = -1 + STOIM_last12 / STOIM_year_before
  ) %>%
  arrange(desc(STOIM_diff))

data_oilgas <- dbGetQuery(
  con,
  "SELECT STRANA, NAPR, PERIOD, NETTO, TNVED4 FROM unified_trade_data"
) %>%
  filter(NAPR == "ЭК", TNVED4 %in% c(oil_tnved4, gas_tnved4)) %>%
  mutate(
    good = if_else(TNVED4 == gas_tnved4, "Природный газ", "Нефть и нефтепродукты"),
    STRANA = if_else(
      STRANA %in% names(strana_oilgas_ru),
      unname(strana_oilgas_ru[STRANA]),
      "Прочие"
    )
  ) %>%
  reframe(NETTO = sum(NETTO, na.rm = TRUE) / 1e6, .by = c(PERIOD, STRANA, good)) %>%
  filter(PERIOD >= ymd("2020-01-01"))

hs4_json <- file.path(repo_root, "metadata", "hs4_labels.json")
if (!file.exists(hs4_json)) {
  hs4_json <- file.path(repo_root, "site", "data", "hs4_labels.json")
}
if (!file.exists(hs4_json)) {
  stop("Missing hs4_labels.json in metadata/ or site/data/")
}
hs4_labels <- fromJSON(hs4_json, flatten = TRUE) %>% as_tibble()

# Заголовочные показатели.
# Стоимость: сумма STOIM по всем странам базы (USD -> млрд), nowcast включён;
# факт считаем отдельно, чтобы показать долю прогноза.
# Физобъём: изменение физического объёма год к году по основным партнёрам
# (Китай, Индия, Турция), согласовано с графиком раздела 1. OTHER/Comtrade
# в индекс не берём: их свежие месяцы обваливаются из-за лага данных.
# Показатели fizob несопоставимы между странами по масштабу, поэтому берём
# YoY по каждой стране отдельно и усредняем с весом по товарообороту (STOIM):
# так вклад страны отражает её долю в торговле, а не масштаб её ряда fizob.
fo_index_headline <- dbGetQuery(
  con,
  "SELECT STRANA, NAPR, PERIOD, fizob FROM fizob_index WHERE tn_level = 0"
) %>%
  filter(STRANA %in% c("CN", "IN", "TR")) %>%
  arrange(STRANA, NAPR, PERIOD) %>%
  mutate(fo_yoy_c = fizob / lag(fizob, 12) - 1, .by = c(STRANA, NAPR)) %>%
  left_join(tab_stoim, by = c("STRANA", "NAPR", "PERIOD")) %>%
  filter(!is.na(fo_yoy_c), !is.na(STOIM), STOIM > 0) %>%
  reframe(fo_yoy = sum(fo_yoy_c * STOIM) / sum(STOIM), .by = c(NAPR, PERIOD)) %>%
  select(NAPR, PERIOD, fo_yoy)

bulletin_headline <- dbGetQuery(
  con,
  "SELECT NAPR, PERIOD, STOIM, NETTO, TYPE FROM unified_trade_data"
) %>%
  mutate(is_fact = TYPE != "pred") %>%
  reframe(
    stoim_bn      = sum(STOIM, na.rm = TRUE) / 1e9,
    stoim_bn_fact = sum(STOIM[is_fact], na.rm = TRUE) / 1e9,
    netto_mt      = sum(NETTO, na.rm = TRUE) / 1e9,
    netto_mt_fact = sum(NETTO[is_fact], na.rm = TRUE) / 1e9,
    .by = c(NAPR, PERIOD)
  ) %>%
  arrange(NAPR, PERIOD) %>%
  mutate(
    nowcast_share = if_else(stoim_bn > 0, 1 - stoim_bn_fact / stoim_bn, NA_real_),
    stoim_mom = stoim_bn / lag(stoim_bn) - 1,
    stoim_yoy = stoim_bn / lag(stoim_bn, 12) - 1,
    netto_mom = netto_mt / lag(netto_mt) - 1,
    netto_yoy = netto_mt / lag(netto_mt, 12) - 1,
    .by = NAPR
  ) %>%
  left_join(fo_index_headline, by = c("NAPR", "PERIOD"))

write_outputs <- function(df, name) {
  path <- file.path(data_dir, name)
  write_parquet(df, path)
  message("  wrote ", path, " (", nrow(df), " rows)")
}

write_outputs(bulletin_fo, "bulletin_fo.parquet")
write_outputs(tab_stoim_oil, "tab_stoim_oil.parquet")
write_outputs(df_groups, "df_groups.parquet")
write_outputs(data_oilgas, "data_oilgas.parquet")
write_outputs(bulletin_headline, "bulletin_headline.parquet")
write_outputs(hs4_labels, "hs4_labels.parquet")

message("bulletin_data_prep: done")
