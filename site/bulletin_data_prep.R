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

tab_stoim_oil <- dbGetQuery(
  con,
  "SELECT STRANA, NAPR, PERIOD, STOIM, TNVED4 FROM unified_trade_data"
) %>%
  filter(NAPR == "ЭК") %>%
  mutate(
    type = if_else(
      TNVED4 %in% c(oil_tnved4, gas_tnved4),
      "Нефтегазовый",
      "Прочие и нераскрытые товары"
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
    STOIM = sum(STOIM, na.rm = TRUE), across(c(NETTO, KOL), ~ sum(.x)),
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
    STOIM = sum(STOIM, na.rm = TRUE), across(c(NETTO, KOL), ~ sum(.x)),
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
  reframe(NETTO = sum(NETTO) / 1e9, .by = c(PERIOD, STRANA, good)) %>%
  filter(PERIOD >= ymd("2020-01-01"))

hs4_json <- file.path(repo_root, "metadata", "hs4_labels.json")
if (!file.exists(hs4_json)) {
  hs4_json <- file.path(repo_root, "site", "data", "hs4_labels.json")
}
if (!file.exists(hs4_json)) {
  stop("Missing hs4_labels.json in metadata/ or site/data/")
}
hs4_labels <- fromJSON(hs4_json, flatten = TRUE) %>% as_tibble() %>%
  mutate(name_ru_short = if_else(hs4 == "2799", "Нераскрытые позиции (2799)", name_ru_short))

# Use the shared Fisher series, including the calculated ALL aggregate.
# Missing indices remain missing; do not average country indices.
source(file.path(repo_root, "site", "read_fizob_bulletin.R"))
fizob_shared <- read_fizob_bulletin(con, repo_root)
bulletin_fo <- fizob_shared %>%
  filter(STRANA %in% c("CN", "IN", "TR"), PERIOD >= as.Date("2020-01-01")) %>%
  transmute(STRANA, NAPR, PERIOD, fizob = yoy)
fo_index_headline <- fizob_shared %>%
  filter(STRANA == "ALL") %>%
  transmute(NAPR, PERIOD, fo_yoy = yoy)

bulletin_headline <- dbGetQuery(
  con,
  "SELECT NAPR, PERIOD, STOIM, NETTO, TYPE FROM unified_trade_data"
) %>%
  mutate(is_fact = TYPE != "pred") %>%
  reframe(
    stoim_bn      = sum(STOIM, na.rm = TRUE) / 1e9,
    stoim_bn_fact = sum(STOIM[is_fact], na.rm = TRUE) / 1e9,
    netto_mt      = sum(NETTO) / 1e9,
    netto_mt_fact = sum(NETTO[is_fact]) / 1e9,
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
