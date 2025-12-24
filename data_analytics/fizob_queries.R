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
#dbDisconnect(con, shutdown = TRUE)

#----------------------------------------
# Конструируем физобъёмы ----------------
#----------------------------------------

# Шаг 1. df - это наша unified_trade_data.db со всеми нужными столбацми.
period_min <- dbGetQuery(con, "SELECT MIN(PERIOD) AS min_period
                                             FROM unified_trade_data") %>% pull(min_period) %>% lubridate::as_date() # "2019-01-01 UTC"

df <- dbGetQuery(con, "
  SELECT STRANA, NAPR, TNVED, PERIOD, EDIZM, STOIM, NETTO, KOL
  FROM unified_trade_data
") %>%
  #filter(STRANA %in% c('AM', 'AD')) %>% # Фильтровал/тесироровал для этих стран для ускорения. 
  arrange(TNVED) %>%
  #filter(any(STOIM > 0), .by = c(STRANA, NAPR, TNVED)) %>% # Здесь я фильтровал базу данных, чтобы убрать группы, для которых все данные 0. Для Индии.
  mutate(PERIOD = as_date(PERIOD)) %>% # Перевожу в date
  group_by(STRANA, TNVED, NAPR) %>%
  complete(
    PERIOD = seq.Date(period_min, dbGetQuery(con, "SELECT MAX(PERIOD) AS max_period
                                             FROM unified_trade_data") %>% pull(max_period) %>% lubridate::as_date(), by = "month"),
    fill = list(
      STOIM = 0,
      KOL = 0,
      NETTO = 0
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

# df_complete = df + значения за базовый период

df_complete <- 
  df %>%
  # Добавляем столбец с датой базисного года для физобъёмов
  left_join(df %>%
              filter(STOIM > 0) %>%
              group_by(STRANA, TNVED, NAPR) %>%
              reframe(first_year_entry = min(PERIOD) %>% floor_date(unit = 'years')),
            by = c('STRANA', 'TNVED', 'NAPR')
            ) %>%
  group_by(STRANA, TNVED, NAPR) %>%
  mutate(
    last_entry = first_year_entry %m+% months(11),
    STOIM_bp = mean(STOIM[PERIOD >= first_year_entry & PERIOD <= last_entry], na.rm = TRUE),
    KOL_bp   = mean(KOL  [PERIOD >= first_year_entry & PERIOD <= last_entry], na.rm = TRUE),
    NETTO_bp = mean(NETTO[PERIOD >= first_year_entry & PERIOD <= last_entry], na.rm = TRUE)
  ) %>%
  ungroup() %>%
  select(-last_entry)

# Единицы измерения

edizm_table <- 
  df_complete %>% 
  group_by(STRANA, TNVED, NAPR) %>%
  reframe(
    unique_edizms = paste(unique(EDIZM), collapse = ", "),
    n_edizm = n_distinct(EDIZM),
    use_netto = any(NETTO > 0 & KOL == 0) # добавил дополнительное условие для проблемных рядов.
  ) %>%
  mutate(fo_constr = if_else(
    unique_edizms %in% c('?', 'NA', NA) | n_edizm > 1 | use_netto,
    'netto',
    'kol'
    )
    ) %>%
  select(STRANA, TNVED, NAPR, fo_constr)

# Таблица с физобъёмами на нижних уровнях (по обычным TNVED)

data_fo <- 
  df_complete %>%
  mutate(TNVED2 = substr(TNVED, start = 1, stop = 2),
         TNVED4 = substr(TNVED, start = 1, stop = 4),
         TNVED6 = substr(TNVED, start = 1, stop = 6)
  ) %>%
  left_join(
    edizm_table,
    by = c('STRANA', 'TNVED', 'NAPR')
  ) %>%
  arrange(STRANA, NAPR, TNVED, PERIOD) %>%
  # Эта часть может быть написана лаконичнее, но я написал группировку/разгруппировку в явном виде.
  group_by(STRANA, NAPR, TNVED2, PERIOD) %>%
  mutate(share_TNVED2 = stoim_12 / sum(stoim_12, na.rm = T)) %>%
  ungroup() %>%
  group_by(STRANA, NAPR, TNVED4, PERIOD) %>%
  mutate(share_TNVED4 = stoim_12 / sum(stoim_12, na.rm = T)) %>%
  ungroup() %>%
  group_by(STRANA, NAPR, TNVED6, PERIOD) %>%
  mutate(share_TNVED6 = stoim_12 / sum(stoim_12, na.rm = T)) %>%
  ungroup() %>%
  mutate(across(
    c(share_TNVED2, share_TNVED4, share_TNVED6),
    ~ .x %>%
      coalesce(0) 
  )
  )

# Проверка, что всё верно.

data_fo %>%
  group_by(STRANA, TNVED4, NAPR, PERIOD) %>%
  reframe(sum_share = sum(share_TNVED4)) %>% 
  filter(sum_share > 1.000000001)


#---------------------------------------------------------------
# Далее формируются 3 таблицы с физобъёмами для 2, 4 и 6 знака -
# --------------------------------------------------------------

#------------------------------
# Посчитаем для второго знака -
#------------------------------

data_fo %>%
  # Для удобства я делаю готовые переменные для физобъёмов
  mutate(fo_unit = if_else(fo_constr == 'netto', netto_12, kol_12),
         fo_unit_bp = if_else(fo_constr == 'netto', NETTO_bp, KOL_bp)
         ) %>%
  # Группировка включает период, это важно
  group_by(STRANA, NAPR, TNVED2, PERIOD) %>%
  # Тут помимо взвешенной суммы мы делаем переменную - базовый период для физобъёма на высоком знаке.
  reframe(fizob2 = sum(fo_unit * share_TNVED2),
          bp = min(first_year_entry)
          ) %>%
  # Считаем значение физобъёма в базовый период. Тут группировка по трём признакам
  mutate(fizob2_bp = mean(fizob2[PERIOD >= bp & PERIOD <= bp %m+% months(11)], na.rm = TRUE),
         .by = c(STRANA, NAPR, TNVED2)
         ) %>%
  # Значения физобъёмов, делённые на среднее значения в базовый год. Тут группировка уже не нужна.
  mutate(fizob2 = fizob2 / fizob2_bp)

#------------------------------------
# На всякий случай, код для 4 знака #
#------------------------------------

data_fo %>%
  mutate(fo_unit = if_else(fo_constr == 'netto', netto_12, kol_12),
         fo_unit_bp = if_else(fo_constr == 'netto', NETTO_bp, KOL_bp)
  ) %>%
  group_by(STRANA, NAPR, TNVED4, PERIOD) %>% # меняется группировка тут
  reframe(fizob4 = sum(fo_unit * share_TNVED4), # и доли тут
          bp = min(first_year_entry)
  ) %>%
  mutate(fizob4_bp = mean(fizob4[PERIOD >= bp & PERIOD <= bp %m+% months(11)], na.rm = TRUE),
         .by = c(STRANA, NAPR, TNVED4) # !
  ) %>%
  mutate(fizob4 = fizob4 / fizob4_bp) #!

#-------------------------------------
# Для 6 знака ------------------------
#-------------------------------------

data_fo %>%
  mutate(fo_unit = if_else(fo_constr == 'netto', netto_12, kol_12),
         fo_unit_bp = if_else(fo_constr == 'netto', NETTO_bp, KOL_bp)
  ) %>%
  group_by(STRANA, NAPR, TNVED6, PERIOD) %>% # меняется группировка тут
  reframe(fizob6 = sum(fo_unit * share_TNVED6), # и доли тут
          bp = min(first_year_entry)
  ) %>%
  mutate(fizob6_bp = mean(fizob6[PERIOD >= bp & PERIOD <= bp %m+% months(11)], na.rm = TRUE),
         .by = c(STRANA, NAPR, TNVED6) # !
  ) %>%
  mutate(fizob6 = fizob6 / fizob6_bp) #!