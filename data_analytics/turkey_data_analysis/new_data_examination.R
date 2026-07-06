library(tidyverse)
library(duckdb)
library(arrow)

con <- dbConnect(
  duckdb::duckdb(),
  "~/MGIMO-FT/db/unified_trade_data.duckdb",
  read_only = TRUE
)

#dbDisconnect(con, shutdown = TRUE) # На всякий случай оставлю тут

# Проверка на совпадение кол-ва наблюдений

data_old <- 
  dbGetQuery(con, "
  SELECT NAPR, STRANA, PERIOD, TYPE, TNVED, STOIM, NETTO, KOL
  FROM unified_trade_data"
  ) %>%
  filter(TYPE == 'fact', 
         STRANA == 'TR') %>%
  mutate(TNVED = substr(TNVED, start = 1, stop = 8)) %>%
  select(-c(TYPE, STRANA)) %>%
  mutate(PERIOD = as_date(PERIOD)) %>%
  filter(PERIOD >= ymd('2019-01-01'),
         PERIOD <= ymd('2025-11-01')
  ) %>%
  reframe(
    across(c(STOIM, NETTO, KOL),
           ~sum(.x, na.rm = T)
    ),
    .by = c('NAPR', 'PERIOD', 'TNVED')
  )

data_new <- read_parquet('turkey-foreign-trade/data/exports/tr_full_compat.parquet') %>%
  select(NAPR, PERIOD, TNVED, STOIM, NETTO, KOL) %>%
  mutate(PERIOD = as_date(PERIOD)) %>%
  filter(PERIOD >= ymd('2019-01-01'),
         PERIOD <= ymd('2025-11-01')
  ) %>%
  reframe(
    across(c(STOIM, NETTO, KOL),
                 ~sum(.x, na.rm = T)
                 ),
          .by = c('NAPR', 'PERIOD', 'TNVED')
          )

data_new %>% nrow() # 301582
data_old %>% nrow() # 280047

#  Вывод: стало больше данных?

#-----------------------------------------
# Теперь проверим, как совпадают данные --
#-----------------------------------------

data_different <- 
  bind_rows(
  data_new %>%
    mutate(type = 'new'),
  data_old %>%
    mutate(type = 'old')
) %>%
  pivot_wider(values_from = c(STOIM, NETTO, KOL),
              names_from = type) %>%
  filter(STOIM_new != STOIM_old |
         NETTO_new != NETTO_old |
         KOL_new != KOL_new
         ) %>%
  mutate(stoim_diff = STOIM_new - STOIM_old,
         netto_diff = NETTO_new - NETTO_old,
         kol_diff = KOL_new - KOL_old
         ) %>%
  arrange(-abs(stoim_diff))

# Вывод: данные отличаются заметно, особенно для кодов 27 и 74.

data_different %>%
  write_parquet('turkey-foreign-trade/R/new_data_examination.parquet')

# Суммы разниц

data_different %>%
  reframe(
    stoim_diff_sum = sum(stoim_diff) / 10^9,
    netto_diff_sum = sum(netto_diff) / 10^9,
    kol_diff_sum = sum(kol_diff) / 10^9
  )