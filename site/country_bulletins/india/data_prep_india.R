library(tidyverse)
library(duckdb)
library(arrow)
library(forecast)
library(plotly)
library(ggbreak)
library(ggtext)

# Какие тут идеи?
# 1. Импорт/экспорт + торговый баланс (барами)
# 2-3. Изменение импорта/экспорта на 4 знаке.

# 1. Торговый баланс + выкладка?

con <- dbConnect(
  duckdb::duckdb(),
  "~/MGIMO-FT/db/unified_trade_data.duckdb",
  read_only = TRUE
)

#dbDisconnect(con, shutdown = TRUE) # На всякий случай оставлю тут

dbGetQuery(con, '
           SELECT STRANA, NAPR, PERIOD, STOIM
           FROM unified_trade_data_enriched') %>%
  filter(STRANA == 'IN') %>%
  reframe(
    STOIM = sum(STOIM, na.rm = TRUE),
    .by = c(STRANA, NAPR, PERIOD)
  ) %>%
  # Создаю табличку с 3 видами по NAPR: ИМ, ЭК, ТБ - это удобно для визуализации
  bind_rows(
    # Табличка с торговым балансом
    dbGetQuery(con, '
           SELECT STRANA, NAPR, PERIOD, STOIM
           FROM unified_trade_data_enriched') %>%
      filter(STRANA == 'IN') %>%
      reframe(
        STOIM = sum(STOIM, na.rm = TRUE),
        .by = c(STRANA, NAPR, PERIOD)
      ) %>%
      pivot_wider(names_from = NAPR,
                  values_from = STOIM) %>%
      mutate(STOIM = ЭК - ИМ,
             NAPR = 'ТБ') %>%
      select(STRANA, NAPR, PERIOD, STOIM)
  ) %>%
  write_parquet('site/country_bulletins/india/data/trade_balance_india.parquet')

# Данные для графиков по товарным группам

dbGetQuery(con, '
           SELECT STRANA, NAPR, PERIOD, STOIM, NETTO, TNVED4
           FROM unified_trade_data') %>%
  # Страна == Индия!
  filter(STRANA == 'IN',
         TNVED4 == '2709') %>%
  # Переводим в млрд $
  mutate(
    STOIM = STOIM / 10^9
  ) %>%
  # Cумма по NETTO и STOIM по группам
  reframe(
    across(
      c(STOIM, NETTO),
      ~ sum(.x, na.rm = T)),
    .by = c(TNVED4, NAPR, PERIOD)
  ) %>%
  # Нужно построит 2 окна, для удобства делаем так
  mutate(
    max_1 = max(PERIOD),
    min_1 = max_1 %m-% months(11),
    max_2 = min_1 %m-% months(1),
    min_2 = max_2 %m-% months(11)
  ) %>%
  # 2 периода: последние 12 мес и 12 мес до этого
  mutate(
    year = case_when(
      (PERIOD >= min_1) ~ 'last12',
      (PERIOD >= min_2) & (PERIOD <= max_2) ~ 'year_before',
      .default = 'other'
    )
  ) %>%
  filter(
    year %in% c('last12', 'year_before')
  ) %>%
  reframe(
    across(
      c(STOIM, NETTO),
      ~ sum(.x, na.rm = T)),
    .by = c(TNVED4, NAPR, year)
  ) %>%
  select(TNVED4, NAPR, year, STOIM, NETTO) %>%
  pivot_wider(names_from = 'year', values_from = c('STOIM', 'NETTO')) %>%
  mutate(STOIM_diff = STOIM_last12 - STOIM_year_before,
         STOIM_gr   = -1 + STOIM_last12 / STOIM_year_before 
  ) %>%
  arrange(-STOIM_diff) %>%
  left_join(
    read_parquet('site/data/hs4_labels.parquet') %>%
      select(TNVED4 = hs4,
             TNVED4_string = name_ru_short),
    by = 'TNVED4'
  ) %>%
  write_parquet('site/country_bulletins/india/data/data_4_india.parquet')

# Нефтяной экспорт NETTO

dbGetQuery(con, '
           SELECT STRANA, NAPR, PERIOD, STOIM, NETTO, TNVED4
           FROM unified_trade_data') %>%
  # Страна == Индия!
  filter(STRANA == 'IN',
         TNVED4 == '2709') %>%
  # Переводим в млрд $
  mutate(
    STOIM = STOIM / 10^6,
    NETTO = NETTO / 10^6
  ) %>%
  # Cумма по NETTO и STOIM по группам
  reframe(
    across(
      c(STOIM, NETTO),
      ~ sum(.x, na.rm = T)),
    .by = c(TNVED4, NAPR, PERIOD)
  )  %>%
  left_join(
    read_parquet('site/data/hs4_labels.parquet') %>%
      select(TNVED4 = hs4,
             TNVED4_string = name_ru_short),
    by = 'TNVED4'
  ) %>%
  write_parquet('site/country_bulletins/india/data/data_oil_export_india.parquet')
  
