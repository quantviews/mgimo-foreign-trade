# Кэширует срезы, которые нужны презентации, но лежат вне git: fizob_2.parquet
# (13 МБ) и база unified_trade_data.duckdb (1,5 ГБ). Благодаря кэшу
# dataviz-story.qmd собирается без запуска полного пайплайна.
#
# Запуск из папки presentations/:
#   Rscript scripts/dataviz_cache.R

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
})

dir.create("figures/dataviz", recursive = TRUE, showWarnings = FALSE)

# --- «спагетти»: 98 групп ТН ВЭД, импорт из Китая ------------------------
src <- "../data_processed/fizob_2.parquet"
if (!file.exists(src)) {
  stop("Нет ", src, ". Сначала пересоберите физобъёмы (см. docs/documentation_fizob.md).")
}

sp <- read_parquet(src) |>
  filter(STRANA == "CN", NAPR == "ИМ", PERIOD >= as.Date("2022-01-01")) |>
  select(TNVED2, PERIOD, fizob2)

write_parquet(sp, "figures/dataviz/spaghetti_cn_import.parquet")
cat("spaghetti_cn_import.parquet — строк:", nrow(sp),
    "· групп ТН ВЭД-2:", length(unique(sp$TNVED2)), "\n")

# --- пример «запрос → график»: экспорт в Китай по месяцам ----------------
# Слайд показывает SQL-запрос и построенный по нему график, поэтому кэшируем
# именно результат этого запроса, а не пересчитываем его другим способом.
db <- "../db/unified_trade_data.duckdb"
if (!file.exists(db)) {
  cat("Нет ", db, " — пример «запрос → график» не пересобран\n", sep = "")
} else {
  suppressPackageStartupMessages({
    library(duckdb)
    library(DBI)
  })
  con <- dbConnect(duckdb(), db, read_only = TRUE)
  ex <- dbGetQuery(con, "
    SELECT PERIOD, ROUND(SUM(STOIM) / 1e9, 2) AS export_bn
    FROM unified_trade_data
    WHERE STRANA = 'CN' AND NAPR = 'ЭК' AND TYPE = 'fact'
      AND PERIOD >= DATE '2022-01-01'
    GROUP BY PERIOD
    ORDER BY PERIOD")
  dbDisconnect(con, shutdown = TRUE)

  write_parquet(ex, "figures/dataviz/example_cn_export.parquet")
  cat("example_cn_export.parquet — месяцев:", nrow(ex),
    "· последний:", format(max(ex$PERIOD)), "\n")
}
