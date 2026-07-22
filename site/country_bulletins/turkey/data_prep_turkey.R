# Готовит parquet-снимки для странового бюллетеня по Турции.
# Запуск из корня репозитория: Rscript site/country_bulletins/turkey/data_prep_turkey.R
# DuckDB: MGIMO_DUCKDB_PATH или db/unified_trade_data.duckdb в корне репо.
#
# Замечание по данным: у Турции энергоэкспорт из России идёт нефтепродуктами
# (2710), а не сырой нефтью (2709) — её в данных нет. Также встречается
# мусорный/перекодированный агрегат 2799 (в справочнике подписан как
# «Субпродукты индейки», крупная стоимость при нулевом тоннаже). В итоговых
# суммах он оставлен (иначе тотал занижается), а из графика «по категориям»
# исключается на стороне qmd.

suppressPackageStartupMessages({
  library(tidyverse)
  library(duckdb)
  library(arrow)
})

`%||%` <- function(x, y) if (!is.null(x) && nzchar(x)) x else y

repo_root <- Sys.getenv("GITHUB_WORKSPACE") %||%
  normalizePath(
    if (basename(getwd()) == "turkey") file.path("..", "..", "..")
    else if (basename(getwd()) == "site") ".."
    else getwd(),
    winslash = "/", mustWork = TRUE
  )

db_path <- Sys.getenv("MGIMO_DUCKDB_PATH") %||%
  file.path(repo_root, "db", "unified_trade_data.duckdb")

out_dir <- file.path(repo_root, "site", "country_bulletins", "turkey", "data")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
hs4_path <- file.path(repo_root, "site", "data", "hs4_labels.parquet")

if (!file.exists(db_path)) {
  stop("DuckDB not found: ", db_path,
       "\nSet MGIMO_DUCKDB_PATH or place db/unified_trade_data.duckdb in the repo root.")
}
message("data_prep_turkey: db=", db_path)
message("data_prep_turkey: out=", out_dir)

con <- dbConnect(duckdb::duckdb(), db_path, read_only = TRUE)
on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)

hs4_labels <- read_parquet(hs4_path) %>%
  select(TNVED4 = hs4, TNVED4_string = name_ru_short)

# 1. Экспорт, импорт и торговый баланс (стоимость; в млрд переводит qmd).
tr_val <- dbGetQuery(
  con, "SELECT NAPR, PERIOD, STOIM FROM unified_trade_data WHERE STRANA = 'TR'"
) %>%
  reframe(STOIM = sum(STOIM, na.rm = TRUE), .by = c(NAPR, PERIOD))

trade_balance <- tr_val %>%
  bind_rows(
    tr_val %>%
      pivot_wider(names_from = NAPR, values_from = STOIM) %>%
      mutate(STOIM = ЭК - ИМ, NAPR = "ТБ") %>%
      select(NAPR, PERIOD, STOIM)
  ) %>%
  mutate(STRANA = "TR") %>%
  select(STRANA, NAPR, PERIOD, STOIM)
write_parquet(trade_balance, file.path(out_dir, "trade_balance_turkey.parquet"))

# 2. Изменение по товарным группам: все группы, последние 12 мес vs 12 мес до.
df_groups <- dbGetQuery(
  con, "SELECT NAPR, PERIOD, STOIM, NETTO, TNVED4 FROM unified_trade_data WHERE STRANA = 'TR'"
) %>%
  mutate(STOIM = STOIM / 1e9) %>%
  reframe(across(c(STOIM, NETTO), ~ sum(.x, na.rm = TRUE)), .by = c(TNVED4, NAPR, PERIOD)) %>%
  mutate(max_1 = max(PERIOD), min_1 = max_1 %m-% months(11),
         max_2 = min_1 %m-% months(1), min_2 = max_2 %m-% months(11)) %>%
  mutate(year = case_when(
    PERIOD >= min_1 ~ "last12",
    PERIOD >= min_2 & PERIOD <= max_2 ~ "year_before",
    .default = "other"
  )) %>%
  filter(year %in% c("last12", "year_before")) %>%
  reframe(across(c(STOIM, NETTO), ~ sum(.x, na.rm = TRUE)), .by = c(TNVED4, NAPR, year)) %>%
  pivot_wider(names_from = year, values_from = c(STOIM, NETTO)) %>%
  mutate(STOIM_diff = STOIM_last12 - STOIM_year_before,
         STOIM_gr = -1 + STOIM_last12 / STOIM_year_before) %>%
  arrange(-STOIM_diff) %>%
  left_join(hs4_labels, by = "TNVED4")
write_parquet(df_groups, file.path(out_dir, "data_4_turkey.parquet"))

# 3. Экспорт нефтепродуктов (2710) в натуральном выражении (млн т).
data_oil <- dbGetQuery(
  con,
  "SELECT NAPR, PERIOD, STOIM, NETTO, TNVED4 FROM unified_trade_data WHERE STRANA = 'TR' AND TNVED4 = '2710'"
) %>%
  mutate(STOIM = STOIM / 1e9, NETTO = NETTO / 1e9) %>%  # млрд $ и млн т
  reframe(across(c(STOIM, NETTO), ~ sum(.x, na.rm = TRUE)), .by = c(TNVED4, NAPR, PERIOD)) %>%
  left_join(hs4_labels, by = "TNVED4")
write_parquet(data_oil, file.path(out_dir, "data_oil_export_turkey.parquet"))

# 4. Заголовочные показатели: стоимость (млрд $), м/м и г/г, доля nowcast,
#    изменение физобъёма год к году.
headline <- dbGetQuery(
  con, "SELECT NAPR, PERIOD, STOIM, TYPE FROM unified_trade_data WHERE STRANA = 'TR'"
) %>%
  mutate(is_fact = TYPE != "pred") %>%
  reframe(
    stoim_bn      = sum(STOIM, na.rm = TRUE) / 1e9,
    stoim_bn_fact = sum(STOIM[is_fact], na.rm = TRUE) / 1e9,
    .by = c(NAPR, PERIOD)
  ) %>%
  arrange(NAPR, PERIOD) %>%
  mutate(
    nowcast_share = if_else(stoim_bn > 0, 1 - stoim_bn_fact / stoim_bn, NA_real_),
    stoim_mom = stoim_bn / lag(stoim_bn) - 1,
    stoim_yoy = stoim_bn / lag(stoim_bn, 12) - 1,
    .by = NAPR
  )

fo_turkey <- dbGetQuery(
  con, "SELECT NAPR, PERIOD, fizob FROM fizob_index WHERE STRANA = 'TR' AND tn_level = 0"
) %>%
  arrange(NAPR, PERIOD) %>%
  mutate(fo_yoy = fizob / lag(fizob, 12) - 1, .by = NAPR) %>%
  select(NAPR, PERIOD, fo_yoy)

headline <- headline %>% left_join(fo_turkey, by = c("NAPR", "PERIOD"))
write_parquet(headline, file.path(out_dir, "headline_turkey.parquet"))

message("data_prep_turkey: done")
