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
  mutate(STOIM = if_else(STRANA == "IN", STOIM / 10^6, STOIM / 10^9))

df_groups <- dbGetQuery(
  con,
  "SELECT STRANA, NAPR, PERIOD, STOIM, NETTO, KOL, TNVED4 FROM unified_trade_data"
) %>%
  mutate(STOIM = if_else(STRANA == "IN", STOIM / 10^6, STOIM / 10^9)) %>%
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

write_outputs <- function(df, name) {
  path <- file.path(data_dir, name)
  write_parquet(df, path)
  message("  wrote ", path, " (", nrow(df), " rows)")
}

write_outputs(bulletin_fo, "bulletin_fo.parquet")
write_outputs(tab_stoim_oil, "tab_stoim_oil.parquet")
write_outputs(df_groups, "df_groups.parquet")
write_outputs(data_oilgas, "data_oilgas.parquet")
write_outputs(hs4_labels, "hs4_labels.parquet")

message("bulletin_data_prep: done")
