# Базовые библиотеки, ничего fancy.
library(tidyverse)
library(slider)
library(duckdb)

con <- dbConnect(
  duckdb::duckdb(),
  "~/MGIMO-FT/db/unified_trade_data.duckdb",
  read_only = TRUE
)

dbGetQuery(con, "SHOW TABLES")

#out
#name
#1           country_reference
#2             tnved_reference
#3          unified_trade_data
#4 unified_trade_data_enriched

# Don't forget to disconnect when done!
dbDisconnect(con, shutdown = TRUE)

#----------------------------------------
# Конструируем физобъёмы ----------------
#----------------------------------------
dbGetQuery(con, "
  SELECT STRANA, NAPR, TNVED, TNVED2, PERIOD, STOIM
  FROM unified_trade_data
") %>%
  arrange(TNVED) %>%
  pull(TNVED) %>%
  unique() %>% tail(1000)

dbGetQuery(con, "
  SELECT STRANA, NAPR, TNVED, TNVED2, PERIOD, STOIM
  FROM unified_trade_data
  WHERE TNVED LIKE '0%'
")

dbGetQuery(con, "
  SELECT STRANA
  FROM fizob_6
") %>%
  nrow()

dbGetQuery(con, "
  SELECT STRANA, NAPR, TNVED, TNVED2, PERIOD, STOIM
  FROM unified_trade_data
") %>%
  arrange(TNVED) %>%
  mutate(
    first_1 = substr(TNVED, start = 1, stop = 1)
  ) %>%
  pull(first_1) %>%
  unique()

dbGetQuery(con, "
  SELECT STRANA, NAPR, TNVED, TNVED2, PERIOD, STOIM
  FROM unified_trade_data
") %>%
  filter(str_starts(TNVED, "0"))

# Шаг 1. df - это наша unified_trade_data.db со всеми нужными столбацми.
period_min <- dbGetQuery(con, "SELECT MIN(PERIOD) AS min_period
                                             FROM fizob_2") %>% pull(min_period) %>% lubridate::as_date() # "2019-01-01 UTC"
period_max <- dbGetQuery(con, "SELECT MAX(PERIOD) AS max_period
                                             FROM fizob_2") %>% pull(max_period) %>% lubridate::as_date()

df <- dbGetQuery(con, "
  SELECT STRANA, NAPR, TNVED2, PERIOD, FIZOB
  FROM fizob_2
") %>%
  
  filter(STRANA %in% c('AM', 'AD')) %>% # Фильтровал/тесироровал для этих стран для ускорения. 
  arrange(TNVED) %>%
  #filter(any(STOIM > 0), .by = c(STRANA, NAPR, TNVED)) %>% # Здесь я фильтровал базу данных, чтобы убрать группы, для которых все данные 0. Для Индии.
  mutate(PERIOD = as_date(PERIOD)) %>% # Перевожу в date
  group_by(STRANA, TNVED, NAPR) %>%
  complete(
    PERIOD = seq.Date(period_min, period_max, by = "month"),
    fill = list(
      FIZON = 0,
    )
  ) %>%
  mutate(EDIZM = first(EDIZM[!is.na(EDIZM)])) %>%
  arrange(STRANA, TNVED, NAPR, PERIOD) %>%
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
    )
  ) %>%
  ungroup()