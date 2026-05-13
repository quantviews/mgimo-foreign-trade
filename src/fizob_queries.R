# Базовые библиотеки, ничего fancy.

library(tidyverse)
library(slider)
library(duckdb)

parse_cli_args <- function(args) {
  config <- list(
    db_path = "db/unified_trade_data.duckdb",
    output_dir = "data_processed"
  )

  if ("--help" %in% args || "-h" %in% args) {
    cat("Usage: Rscript src/fizob_queries.R [--db-path PATH] [--output-dir DIR]\n")
    quit(status = 0)
  }

  i <- 1
  while (i <= length(args)) {
    arg <- args[[i]]
    if (arg %in% c("--db-path", "--output-dir")) {
      if (i == length(args)) {
        stop(sprintf("Missing value for %s", arg), call. = FALSE)
      }
      value <- args[[i + 1]]
      if (arg == "--db-path") {
        config$db_path <- value
      } else {
        config$output_dir <- value
      }
      i <- i + 2
    } else {
      stop(sprintf("Unknown argument: %s", arg), call. = FALSE)
    }
  }

  config
}

config <- parse_cli_args(commandArgs(trailingOnly = TRUE))
dir.create(config$output_dir, recursive = TRUE, showWarnings = FALSE)

run_started_at <- Sys.time()

fmt_n <- function(x) {
  format(x, big.mark = ",", scientific = FALSE, trim = TRUE)
}

log_step <- function(message) {
  cat(
    sprintf(
      "[fizob] %s | %s\n",
      format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      message
    )
  )
  flush.console()
}

log_table <- function(name, data) {
  log_step(sprintf("%s: rows=%s cols=%s", name, fmt_n(nrow(data)), ncol(data)))
}

sql_date <- function(value) {
  sprintf("DATE '%s'", as.character(value))
}

cleanup_objects <- function(...) {
  objects <- c(...)
  existing <- objects[objects %in% ls(envir = .GlobalEnv)]
  if (length(existing) == 0) {
    return(invisible(NULL))
  }

  rm(list = existing, envir = .GlobalEnv)
  invisible(gc())
  log_step(sprintf("Released objects: %s", paste(existing, collapse = ", ")))
}

log_step("Starting fizob calculation")
log_step(sprintf("Connecting to %s", config$db_path))
log_step(sprintf("Output directory: %s", config$output_dir))

con <- dbConnect(
  duckdb::duckdb(),
  config$db_path,
  read_only = TRUE
)

tables <- dbGetQuery(con, "SHOW TABLES")
log_step(sprintf("DuckDB objects: %s", paste(tables$name, collapse = ", ")))

#out
#name
#1           country_reference
#2             tnved_reference
#3          unified_trade_data
#4 unified_trade_data_enriched

# Don't forget to disconnect when done!
#dbDisconnect(con, shutdown = TRUE)

#----------------------------------------
# Конструируем физобъёмы ----------------
#----------------------------------------

# Шаг 1. df - это наша unified_trade_data.db со всеми нужными столбацми.
log_step("Reading fact period range")
period_min <- dbGetQuery(con, "SELECT MIN(PERIOD) AS min_period
                                             FROM unified_trade_data
                                             WHERE TYPE = 'fact'") %>% pull(min_period) %>% lubridate::as_date() # "2019-01-01 UTC"
period_max <- dbGetQuery(con, "SELECT MAX(PERIOD) AS max_period
                                             FROM unified_trade_data
                                             WHERE TYPE = 'fact'") %>% pull(max_period) %>% lubridate::as_date()
log_step(sprintf("Fact period range: %s to %s", period_min, period_max))

log_step("Building complete monthly grid in DuckDB")
invisible(dbExecute(con, sprintf("
  CREATE OR REPLACE TEMP TABLE fizob_fact AS
  SELECT
    STRANA,
    NAPR,
    TNVED,
    CAST(PERIOD AS DATE) AS PERIOD,
    EDIZM,
    STOIM,
    NETTO,
    KOL
  FROM unified_trade_data
  WHERE TYPE = 'fact'
")))

invisible(dbExecute(con, "
  CREATE OR REPLACE TEMP TABLE fizob_keys AS
  SELECT STRANA, TNVED, NAPR
  FROM fizob_fact
  GROUP BY STRANA, TNVED, NAPR
  HAVING BOOL_OR(STOIM > 0)
"))

invisible(dbExecute(con, sprintf("
  CREATE OR REPLACE TEMP TABLE fizob_months AS
  SELECT CAST(period_value AS DATE) AS PERIOD
  FROM generate_series(%s, %s, INTERVAL 1 MONTH) AS months(period_value)
", sql_date(period_min), sql_date(period_max))))

invisible(dbExecute(con, "
  CREATE OR REPLACE TEMP TABLE fizob_key_meta AS
  SELECT
    k.STRANA,
    k.TNVED,
    k.NAPR,
    ARG_MIN(f.EDIZM, f.PERIOD) FILTER (WHERE f.EDIZM IS NOT NULL) AS EDIZM,
    COUNT(DISTINCT f.EDIZM) FILTER (WHERE f.EDIZM IS NOT NULL) AS n_edizm,
    BOOL_OR(COALESCE(f.NETTO, 0) > 0 AND COALESCE(f.KOL, 0) = 0) AS use_netto,
    DATE_TRUNC('year', MIN(f.PERIOD) FILTER (WHERE f.STOIM > 0)) AS first_year_entry
  FROM fizob_keys k
  LEFT JOIN fizob_fact f
    ON k.STRANA = f.STRANA
   AND k.TNVED = f.TNVED
   AND k.NAPR = f.NAPR
  GROUP BY k.STRANA, k.TNVED, k.NAPR
"))

grid_metrics <- dbGetQuery(con, "
  SELECT
    (SELECT COUNT(*) FROM fizob_fact) AS fact_rows,
    (SELECT COUNT(*) FROM fizob_keys) AS nonzero_series,
    (SELECT COUNT(*) FROM fizob_keys) * (SELECT COUNT(*) FROM fizob_months) AS complete_grid_rows
")
log_step(sprintf(
  "DuckDB grid inputs: fact rows=%s non-zero series=%s complete grid rows=%s",
  fmt_n(grid_metrics$fact_rows),
  fmt_n(grid_metrics$nonzero_series),
  fmt_n(grid_metrics$complete_grid_rows)
))

df <- dbGetQuery(con, "
  SELECT
    k.STRANA,
    k.NAPR,
    k.TNVED,
    m.PERIOD,
    meta.EDIZM,
    COALESCE(f.STOIM, 0) AS STOIM,
    COALESCE(f.NETTO, 0) AS NETTO,
    COALESCE(f.KOL, 0) AS KOL,
    CASE
      WHEN meta.EDIZM IN ('?', 'NA')
        OR meta.EDIZM IS NULL
        OR meta.n_edizm > 1
        OR meta.use_netto
      THEN 'netto'
      ELSE 'kol'
    END AS fo_constr,
    meta.first_year_entry
  FROM fizob_keys k
  CROSS JOIN fizob_months m
  LEFT JOIN fizob_fact f
    ON k.STRANA = f.STRANA
   AND k.TNVED = f.TNVED
   AND k.NAPR = f.NAPR
   AND m.PERIOD = f.PERIOD
  LEFT JOIN fizob_key_meta meta
    ON k.STRANA = meta.STRANA
   AND k.TNVED = meta.TNVED
   AND k.NAPR = meta.NAPR
  ORDER BY k.STRANA, k.TNVED, k.NAPR, m.PERIOD
") %>%
  mutate(
    PERIOD = as_date(PERIOD),
    first_year_entry = as_date(first_year_entry)
  )
log_table("df_complete_grid", df)

# df_complete = df + значения за базовый период

log_step("Calculating prices, rolling windows and base-period values")
df_complete <-
  df %>%
  # Делаем цену: она нужна для заполнений случаев, если есть только STOIM, а KOL и NETTO == 0.
  mutate(price = if_else(fo_constr == 'netto', STOIM / NETTO, STOIM / KOL)) %>%
  group_by(STRANA, TNVED, NAPR) %>%
  arrange(PERIOD) %>%
  # На самом деле, окно 13 мес.
  mutate(
    price_12 = slide_dbl(
      price,
      .before = 11,#6,
      #.after  = 6,
      .complete = FALSE,
      .f = ~ {
        x <- .x[is.finite(.x) & .x != 0]
        if (length(x) == 0) NA_real_ else mean(x)
      }
    )
  ) %>%
  # Заполнение пропусков по группам ближайшим значением
  fill(price_12, .direction = "downup") %>%
  filter(any(price_12 > 0)) %>%
  ungroup() %>%
  # Важно! Здесь я ЗАМЕНЯЮ старые KOL и NETTO, Если STOIM > 0, а KOL и NETTO == 0. Замены происходят в редких случаях, тем не менее, дальше KOL и NETTO - не исходные.
  mutate(
    KOL = if_else( 
      (STOIM > 0) & !(KOL > 0),
      STOIM / price_12,
      KOL),
    NETTO = if_else(
      (STOIM > 0) & !(NETTO > 0),
      STOIM / price_12,
      NETTO
    ),
    # Для удобства
    price_edizm = if_else(fo_constr == 'netto', 'Долл./кг', paste0('Долл./', EDIZM))
  ) %>%
  group_by(STRANA, TNVED, NAPR) %>%
  # На самом деле, окно 13 мес.
  mutate(
    kol_12 = slide_dbl(
      KOL, 
      .f = mean,
      .before = 11,
      .complete = FALSE
    ),
    netto_12 = slide_dbl(
      NETTO, 
      .f = mean,
      .before = 11,
      .complete = FALSE
    ),
    stoim_12 = slide_dbl(
      STOIM,
      .f = mean,
      .before = 11,
      .complete = FALSE
    ),
    # Значения в базовый период
    last_entry = first_year_entry %m+% months(11),
    STOIM_bp = mean(STOIM[PERIOD >= first_year_entry & PERIOD <= last_entry], na.rm = TRUE),
    KOL_bp   = mean(KOL  [PERIOD >= first_year_entry & PERIOD <= last_entry], na.rm = TRUE),
    NETTO_bp = mean(NETTO[PERIOD >= first_year_entry & PERIOD <= last_entry], na.rm = TRUE)
  ) %>%
  ungroup() %>%
  select(-last_entry)
log_table("df_complete", df_complete)
cleanup_objects("df")
  
# Таблицы с физобъёмами на нижних уровнях считаются в DuckDB:
# shares, TNVED2/4/6, total и ALL-агрегаты не материализуются в R.

query_fizob_level <- function(con, level) {
  code_col <- paste0("TNVED", level)
  fizob_col <- paste0("fizob", level)
  price_col <- paste0("price", level)
  fizob_bp_col <- paste0("fizob", level, "_bp")
  price_bp_col <- paste0("price", level, "_bp")
  share_col <- paste0("share_TNVED", level)

  dbGetQuery(con, sprintf("
    WITH agg AS (
      SELECT
        STRANA,
        NAPR,
        %1$s,
        PERIOD,
        SUM((CASE WHEN fo_constr = 'netto' THEN netto_12 ELSE kol_12 END) * %6$s) AS raw_fizob,
        SUM(price_12 * %6$s) AS raw_price,
        MIN(first_year_entry) AS bp
      FROM data_fo
      GROUP BY STRANA, NAPR, %1$s, PERIOD
    ),
    norm AS (
      SELECT
        *,
        AVG(raw_fizob) FILTER (
          WHERE PERIOD >= bp AND PERIOD <= bp + INTERVAL 11 MONTH
        ) OVER (PARTITION BY STRANA, NAPR, %1$s) AS raw_fizob_bp,
        AVG(raw_price) FILTER (
          WHERE PERIOD >= bp AND PERIOD <= bp + INTERVAL 11 MONTH AND raw_price > 0
        ) OVER (PARTITION BY STRANA, NAPR, %1$s) AS raw_price_bp
      FROM agg
    )
    SELECT
      STRANA,
      NAPR,
      %1$s,
      PERIOD,
      raw_fizob / raw_fizob_bp AS %2$s,
      raw_price / raw_price_bp AS %3$s,
      bp,
      raw_fizob_bp AS %4$s,
      raw_price_bp AS %5$s
    FROM norm
    ORDER BY STRANA, NAPR, %1$s, PERIOD
  ", code_col, fizob_col, price_col, fizob_bp_col, price_bp_col, share_col))
}

query_fizob_all_level <- function(con, level) {
  code_col <- paste0("TNVED", level)
  fizob_col <- paste0("fizob", level)
  price_col <- paste0("price", level)
  fizob_bp_col <- paste0("fizob", level, "_bp")
  price_bp_col <- paste0("price", level, "_bp")
  share_col <- paste0("share_TNVED", level)

  dbGetQuery(con, sprintf("
    WITH agg AS (
      SELECT
        NAPR,
        %1$s,
        PERIOD,
        SUM(netto_12 * %6$s) AS raw_fizob,
        SUM(price_12 * %6$s) AS raw_price,
        MIN(first_year_entry) AS bp
      FROM data_fo_all
      GROUP BY NAPR, %1$s, PERIOD
    ),
    norm AS (
      SELECT
        *,
        AVG(raw_fizob) FILTER (
          WHERE PERIOD >= bp AND PERIOD <= bp + INTERVAL 11 MONTH
        ) OVER (PARTITION BY NAPR, %1$s) AS raw_fizob_bp,
        AVG(raw_price) FILTER (
          WHERE PERIOD >= bp AND PERIOD <= bp + INTERVAL 11 MONTH AND raw_price > 0
        ) OVER (PARTITION BY NAPR, %1$s) AS raw_price_bp
      FROM agg
    )
    SELECT
      'ALL' AS STRANA,
      NAPR,
      %1$s,
      PERIOD,
      raw_fizob / raw_fizob_bp AS %2$s,
      raw_price / raw_price_bp AS %3$s,
      bp,
      raw_fizob_bp AS %4$s,
      raw_price_bp AS %5$s
    FROM norm
    ORDER BY NAPR, %1$s, PERIOD
  ", code_col, fizob_col, price_col, fizob_bp_col, price_bp_col, share_col))
}

log_step("Registering df_complete in DuckDB")
duckdb_register(con, "df_complete_r", df_complete)

log_step("Building TNVED2/4/6 shares in DuckDB")
invisible(dbExecute(con, "
  CREATE OR REPLACE TEMP TABLE data_fo AS
  WITH base AS (
    SELECT
      *,
      SUBSTR(TNVED, 1, 2) AS TNVED2,
      SUBSTR(TNVED, 1, 4) AS TNVED4,
      SUBSTR(TNVED, 1, 6) AS TNVED6
    FROM df_complete_r
  ),
  denominators AS (
    SELECT
      *,
      SUM(stoim_12) OVER (
        PARTITION BY STRANA, NAPR, TNVED2, PERIOD
      ) AS sum_stoim_TNVED2,
      SUM(stoim_12) OVER (
        PARTITION BY STRANA, NAPR, TNVED4, PERIOD
      ) AS sum_stoim_TNVED4,
      SUM(stoim_12) OVER (
        PARTITION BY STRANA, NAPR, TNVED6, PERIOD
      ) AS sum_stoim_TNVED6
    FROM base
  )
  SELECT
    *,
    CASE WHEN sum_stoim_TNVED2 = 0 THEN 0 ELSE stoim_12 / sum_stoim_TNVED2 END AS share_TNVED2,
    CASE WHEN sum_stoim_TNVED4 = 0 THEN 0 ELSE stoim_12 / sum_stoim_TNVED4 END AS share_TNVED4,
    CASE WHEN sum_stoim_TNVED6 = 0 THEN 0 ELSE stoim_12 / sum_stoim_TNVED6 END AS share_TNVED6
  FROM denominators
"))

data_fo_rows <- dbGetQuery(con, "SELECT COUNT(*) AS rows FROM data_fo")$rows
log_step(sprintf("data_fo DuckDB temp table: rows=%s", fmt_n(data_fo_rows)))

share_check_rows <- dbGetQuery(con, "
  SELECT COUNT(*) AS rows
  FROM (
    SELECT STRANA, TNVED4, NAPR, PERIOD, SUM(share_TNVED4) AS sum_share
    FROM data_fo
    GROUP BY STRANA, TNVED4, NAPR, PERIOD
    HAVING SUM(share_TNVED4) > 1.000000001
  )
")$rows
log_step(sprintf("Share check violations: %s", fmt_n(share_check_rows)))

log_step("Calculating fizob level TNVED2 in DuckDB")
fo_2 <- query_fizob_level(con, 2)
log_table("fo_2", fo_2)

log_step("Calculating fizob level TNVED4 in DuckDB")
fo_4 <- query_fizob_level(con, 4)
log_table("fo_4", fo_4)

log_step("Calculating fizob level TNVED6 in DuckDB")
fo_6 <- query_fizob_level(con, 6)
log_table("fo_6", fo_6)

log_step("Calculating total fizob by country and direction in DuckDB")
fo_tot <- dbGetQuery(con, "
  WITH agg AS (
    SELECT
      STRANA,
      NAPR,
      PERIOD,
      SUM(CASE WHEN fo_constr = 'netto' THEN netto_12 ELSE kol_12 END) AS raw_fizob,
      MIN(first_year_entry) AS bp
    FROM data_fo
    GROUP BY STRANA, NAPR, PERIOD
  ),
  norm AS (
    SELECT
      *,
      AVG(raw_fizob) FILTER (
        WHERE PERIOD >= bp AND PERIOD <= bp + INTERVAL 11 MONTH
      ) OVER (PARTITION BY STRANA, NAPR) AS raw_fizob_bp
    FROM agg
  )
  SELECT
    STRANA,
    NAPR,
    PERIOD,
    raw_fizob / raw_fizob_bp AS fizob,
    bp,
    raw_fizob_bp AS fizob_bp
  FROM norm
  ORDER BY STRANA, NAPR, PERIOD
")
log_table("fo_tot", fo_tot)

log_step("Building ALL-country aggregate shares in DuckDB")
invisible(dbExecute(con, "
  CREATE OR REPLACE TEMP TABLE data_fo_all AS
  WITH base AS (
    SELECT
      *,
      SUBSTR(TNVED, 1, 2) AS TNVED2,
      SUBSTR(TNVED, 1, 4) AS TNVED4,
      SUBSTR(TNVED, 1, 6) AS TNVED6
    FROM df_complete_r
  ),
  denominators AS (
    SELECT
      *,
      SUM(stoim_12) OVER (
        PARTITION BY NAPR, TNVED2, PERIOD
      ) AS sum_stoim_TNVED2,
      SUM(stoim_12) OVER (
        PARTITION BY NAPR, TNVED4, PERIOD
      ) AS sum_stoim_TNVED4,
      SUM(stoim_12) OVER (
        PARTITION BY NAPR, TNVED6, PERIOD
      ) AS sum_stoim_TNVED6
    FROM base
  )
  SELECT
    *,
    CASE WHEN sum_stoim_TNVED2 = 0 THEN 0 ELSE stoim_12 / sum_stoim_TNVED2 END AS share_TNVED2,
    CASE WHEN sum_stoim_TNVED4 = 0 THEN 0 ELSE stoim_12 / sum_stoim_TNVED4 END AS share_TNVED4,
    CASE WHEN sum_stoim_TNVED6 = 0 THEN 0 ELSE stoim_12 / sum_stoim_TNVED6 END AS share_TNVED6
  FROM denominators
"))

data_fo_all_rows <- dbGetQuery(con, "SELECT COUNT(*) AS rows FROM data_fo_all")$rows
log_step(sprintf("data_fo_all DuckDB temp table: rows=%s", fmt_n(data_fo_all_rows)))

log_step("Calculating ALL-country fizob level TNVED2 in DuckDB")
fo_2_gr <- query_fizob_all_level(con, 2)
log_table("fo_2_gr", fo_2_gr)

log_step("Calculating ALL-country fizob level TNVED4 in DuckDB")
fo_4_gr <- query_fizob_all_level(con, 4)
log_table("fo_4_gr", fo_4_gr)

log_step("Calculating ALL-country fizob level TNVED6 in DuckDB")
fo_6_gr <- query_fizob_all_level(con, 6)
log_table("fo_6_gr", fo_6_gr)

try(duckdb_unregister(con, "df_complete_r"), silent = TRUE)
invisible(dbExecute(con, "DROP TABLE IF EXISTS data_fo"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS data_fo_all"))
cleanup_objects("df_complete")

fo_2 <- bind_rows(fo_2, fo_2_gr)
fo_4 <- bind_rows(fo_4, fo_4_gr)
fo_6 <- bind_rows(fo_6, fo_6_gr)
log_table("fo_2_final", fo_2)
log_table("fo_4_final", fo_4)
log_table("fo_6_final", fo_6)
cleanup_objects("fo_2_gr", "fo_4_gr", "fo_6_gr")

#------------------------------------------
# Сохранение результатов в parquet файлах -
#------------------------------------------

log_step("Disconnecting DuckDB")
dbDisconnect(con, shutdown = TRUE) # Закрываем соединение

write_fizob_parquet <- function(data, path) {
  log_step(sprintf("Writing %s: rows=%s", path, fmt_n(nrow(data))))
  arrow::write_parquet(data, path, compression = "zstd", compression_level = 5L)
  log_step(sprintf("Saved %s: size=%s bytes", path, fmt_n(file.info(path)$size)))
}

# zstd даёт заметно меньше размер файла, чем snappy по умолчанию
write_fizob_parquet(fo_2, file.path(config$output_dir, 'fizob_2.parquet'))
cleanup_objects("fo_2")
write_fizob_parquet(fo_4, file.path(config$output_dir, 'fizob_4.parquet'))
cleanup_objects("fo_4")
write_fizob_parquet(fo_6, file.path(config$output_dir, 'fizob_6.parquet'))
cleanup_objects("fo_6")
write_fizob_parquet(
  fo_tot %>% mutate(TNVED2 = '0'),
  file.path(config$output_dir, 'fizob_total.parquet')
)
cleanup_objects("fo_tot")

elapsed_seconds <- as.numeric(difftime(Sys.time(), run_started_at, units = "secs"))
log_step(sprintf("Finished fizob calculation in %.1f seconds", elapsed_seconds))
