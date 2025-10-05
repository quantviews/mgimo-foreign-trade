library(DBI)
library(duckdb)
library(dplyr)
library(dbplyr)

file <- 'db/unified_trade_data.duckdb'

con <- dbConnect(duckdb::duckdb(), dbdir = file, read_only = TRUE)

trade <- tbl(con, "unified_trade_data")

# dbplyr chain
monthly_by_country <- trade %>%
  mutate(
    # DuckDB understands date_trunc directly
    month = sql("date_trunc('month', CAST(PERIOD AS TIMESTAMP))"),
    direction = case_when(
      NAPR %in% c('ЭК','/ЭК','Э') ~ "export",
      NAPR %in% c('ИМ','/ИМ','И') ~ "import",
      TRUE ~ "other"
    )
  ) %>%
  group_by(month, STRANA, direction) %>%
  summarise(
    value  = sum(STOIM, na.rm = TRUE),
    weight = sum(NETTO, na.rm = TRUE),
    .groups = "drop"
  )

# This is still a lazy dbplyr query object
df = monthly_by_country
