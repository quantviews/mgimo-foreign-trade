library(tidyverse)
library(duckdb)
library(arrow)
library(forecast)
library(plotly)
library(ggbreak)
library(ggtext)

con <- dbConnect(
  duckdb::duckdb(),
  "~/MGIMO-FT/db/unified_trade_data.duckdb",
  read_only = TRUE
)

#dbDisconnect(con, shutdown = TRUE) # На всякий случай оставлю тут

replace_outlier <- function(x, frequency = 12) {
  as.numeric(tsclean(ts(x, frequency = frequency)))
}



dbListFields(con, "fizob_index")

tab_stoim <- dbGetQuery(con, '
           SELECT STRANA, NAPR, PERIOD, STOIM
           FROM unified_trade_data') %>%
  reframe(
    STOIM = sum(STOIM, na.rm = T),
    .by = c(STRANA, NAPR, PERIOD)
  )

dbGetQuery(con, '
           SELECT STRANA, NAPR, PERIOD, tn_level, tn_code, fizob, fizob_bp
           FROM fizob_index')  %>%
  filter(tn_level == 0) %>%
  filter(STRANA %in% STRANA[fizob > 10000]) %>%
  pull(STRANA) %>%
  unique() -> strana_out

# Данные для графика по физобъёмам (выкладка, 4)

bulletin_fo <- 
  dbGetQuery(con, '
           SELECT STRANA, NAPR, PERIOD, tn_level, tn_code, fizob, fizob_bp
           FROM fizob_index') %>%
  filter(STRANA %in% setdiff(STRANA, strana_out)) %>%
  filter(tn_level == 0) %>%
  left_join(tab_stoim,
            by = c('STRANA', 'NAPR', 'PERIOD')
            ) %>%
  mutate(
    STRANA = if_else(STRANA %in% c('IN', 'TR', 'CN'),
                     STRANA,
                     'OTHER'),
    fizob = if_else(STRANA == 'OTHER',
                    fizob * STOIM,
                    fizob)
    ) %>%
  reframe(
    fizob =  mean(fizob, na.rm = T),
    .by = c(STRANA, NAPR, PERIOD)
  ) %>%
  arrange(STRANA, NAPR, PERIOD) %>%
  # Удаляем выбросы
  mutate(
    fizob = if_else(STRANA == 'OTHER', 
                    replace_outlier(fizob),
                    fizob),
    .by = c(STRANA, NAPR)
  ) %>%
  # делаем г/г
  mutate(
    fizob = -1 + fizob / lag(fizob, 12),
    .by = c(STRANA, NAPR)
  ) %>%
  filter(PERIOD >= ymd('2020-01-01'))

bulletin_fo %>%
  write_parquet('site/data/bulletin_fo.parquet')

# Табличка с данными по нефтегазовому и ненефтегазовому экспорту

tab_stoim_oil <- dbGetQuery(con, '
           SELECT STRANA, NAPR, PERIOD, STOIM, TNVED4
           FROM unified_trade_data') %>%
  filter(NAPR == 'ЭК') %>%
  # Нефтяной и ненефтяной экспорт
  mutate(
    type = if_else(
      TNVED4 %in% c(
        "2709", # Нефть сырая и нефтепродукты сырые
        "2710", # Нефтепродукты, масла нефтяные (бензин, дизель, мазут и др.)
        "2711", # Нефтяные газы и прочие газообразные углеводороды
        "2712", # Вазелин, парафин, нефтяной воск и т.п.
        "2713", # Нефтяной кокс, битум и прочие остатки
        "2714", # Битумы и асфальты природные
        "2715"  # Битумные смеси на основе природного асфальта или нефтяного битума
      ),
      'Нефтегазовый',
      'Кроме нефти и газа'
    ),
    STRANA = if_else(STRANA %in% c('IN', 'TR', 'CN'),
                     STRANA,
                     'OTHER')
  ) %>%
  reframe(
    STOIM = sum(STOIM, na.rm = T),
    .by = c(STRANA, NAPR, PERIOD, type)
  ) %>%
  mutate(
    STOIM = if_else(STRANA == 'IN', STOIM / 10^6, STOIM /10^9)
  )

tab_stoim_oil %>%
  write_parquet('site/data/tab_stoim_oil.parquet')

# График с чем-то интересным.
# Карта? Выкладка с разными типами графиков?

df_groups <-
dbGetQuery(con, '
           SELECT STRANA, NAPR, PERIOD, STOIM, NETTO, KOL, TNVED4
           FROM unified_trade_data') %>%
  mutate(
    STOIM = if_else(STRANA == 'IN', STOIM / 10^6, STOIM / 10^9)
  ) %>%
  reframe(
    across(
      c(STOIM, NETTO, KOL),
      ~ sum(.x, na.rm = T)),
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
      c(STOIM, NETTO, KOL),
      ~ sum(.x, na.rm = T)),
    .by = c(TNVED4, NAPR, year)
  ) %>%
  select(TNVED4, NAPR, year, STOIM, NETTO, KOL) %>%
  pivot_wider(names_from = 'year', values_from = c('STOIM', 'NETTO', 'KOL')) %>%
  mutate(STOIM_diff = STOIM_last12 - STOIM_year_before,
         STOIM_gr   = -1 + STOIM_last12 / STOIM_year_before 
         ) %>%
  arrange(-STOIM_diff)

df_groups %>%
  write_parquet('site/data/df_groups.parquet')
