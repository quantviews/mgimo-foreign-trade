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
period_max <- dbGetQuery(con, "SELECT MAX(PERIOD) AS max_period
                                             FROM unified_trade_data") %>% pull(max_period) %>% lubridate::as_date()

df <- dbGetQuery(con, "
  SELECT STRANA, NAPR, TNVED, PERIOD, EDIZM, STOIM, NETTO, KOL
  FROM unified_trade_data
") %>%
  filter(any(STOIM > 0), .by = c(STRANA, NAPR, TNVED)) %>% # Здесь я фильтровал базу данных, чтобы убрать группы, для которых все данные 0. Для Индии.
  mutate(PERIOD = as_date(PERIOD)) %>% # Перевожу в date
  # Заполняю пропуски без группировки - замена для group_by %>% complete. Минут на 5 быстрее
  right_join(
    dbGetQuery(con, "
  SELECT STRANA, NAPR, TNVED, PERIOD, EDIZM, STOIM, NETTO, KOL
  FROM unified_trade_data
               ") %>%
      filter(any(STOIM > 0), .by = c(STRANA, NAPR, TNVED)) %>%
      distinct(STRANA, TNVED, NAPR) %>%
      cross_join(
        data.frame(
          PERIOD = seq.Date(period_min, period_max, by = "month")
        )
      ),
    by = c('STRANA', 'NAPR', 'TNVED', 'PERIOD')
  ) %>%
  mutate(EDIZM = first(EDIZM[!is.na(EDIZM)]),
         .by = c(STRANA, NAPR, TNVED)) %>%
  mutate(
    STOIM = coalesce(STOIM, 0),
    KOL   = coalesce(KOL, 0),
    NETTO = coalesce(NETTO, 0)
  ) %>%
  arrange(STRANA, TNVED, NAPR, PERIOD)

# Единицы измерения

edizm_table <- 
  df %>%
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

# df_complete = df + значения за базовый период

df_complete <-
  df %>%
  # Добавляем единицы измерения
  left_join(
    edizm_table,
    by = c('STRANA', 'TNVED', 'NAPR')
  ) %>%
  # Делаем цену: она нужна для заполнений случаев, если есть только STOIM, а KOL и NETTO == 0.
  mutate(price = if_else(fo_constr == 'netto', STOIM / NETTO, STOIM / KOL)) %>%
  # Добавляем столбец с датой базисного года для физобъёмов
  left_join(df %>%
              filter(STOIM > 0) %>%
              group_by(STRANA, TNVED, NAPR) %>%
              reframe(first_year_entry = min(PERIOD) %>% floor_date(unit = 'years')),
            by = c('STRANA', 'TNVED', 'NAPR')
  ) %>%
  group_by(STRANA, TNVED, NAPR) %>%
  arrange(PERIOD) %>%
  # На самом деле, окно 13 мес.
  mutate(
    price_12 = slide_dbl(
      price,
      .before = 6,
      .after  = 6,
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
  
# Таблица с физобъёмами на нижних уровнях (по обычным TNVED)

data_fo <- 
  df_complete %>%
  mutate(TNVED2 = substr(TNVED, start = 1, stop = 2),
         TNVED4 = substr(TNVED, start = 1, stop = 4),
         TNVED6 = substr(TNVED, start = 1, stop = 6)
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

fo_2 <-
  data_fo %>%
  # Для удобства я делаю готовые переменные для физобъёмов
  mutate(fo_unit = if_else(fo_constr == 'netto', netto_12, kol_12),
         fo_unit_bp = if_else(fo_constr == 'netto', NETTO_bp, KOL_bp)
  ) %>%
  # Группировка включает период, это важно
  group_by(STRANA, NAPR, TNVED2, PERIOD) %>%
  # Тут помимо взвешенной суммы мы делаем переменную - базовый период для физобъёма на высоком знаке.
  reframe(fizob2 = sum(fo_unit * share_TNVED2),
          price2 = sum(price_12 * share_TNVED2),
          bp = min(first_year_entry)
  ) %>%
  # Считаем значение физобъёма в базовый период. Тут группировка по трём признакам
  mutate(fizob2_bp = mean(fizob2[PERIOD >= bp & PERIOD <= bp %m+% months(11)], na.rm = TRUE),
         price2_bp = mean(price2[PERIOD >= bp & PERIOD <= bp %m+% months(11) & price2 > 0], na.rm = TRUE),
         .by = c(STRANA, NAPR, TNVED2)
  ) %>%
  # Значения физобъёмов, делённые на среднее значения в базовый год. Тут группировка уже не нужна.
  mutate(fizob2 = fizob2 / fizob2_bp,
         price2 = price2 / price2_bp)

#------------------------------------
# Для 4 знака -----------------------
#------------------------------------

fo_4 <-
  data_fo %>%
  mutate(fo_unit = if_else(fo_constr == 'netto', netto_12, kol_12),
         fo_unit_bp = if_else(fo_constr == 'netto', NETTO_bp, KOL_bp)
  ) %>%
  group_by(STRANA, NAPR, TNVED4, PERIOD) %>% # меняется группировка тут
  reframe(fizob4 = sum(fo_unit * share_TNVED4), # и доли тут
          price4 = sum(price_12 * share_TNVED4),
          bp = min(first_year_entry)
  ) %>%
  mutate(fizob4_bp = mean(fizob4[PERIOD >= bp & PERIOD <= bp %m+% months(11)], na.rm = TRUE),
         price4_bp = mean(price4[PERIOD >= bp & PERIOD <= bp %m+% months(11) & price4 > 0], na.rm = TRUE),
         .by = c(STRANA, NAPR, TNVED4) # !
  ) %>%
  mutate(fizob4 = fizob4 / fizob4_bp,
         price4 = price4 / price4_bp) #!

#-------------------------------------
# Для 6 знака ------------------------
#-------------------------------------

fo_6 <-
  data_fo %>%
  mutate(fo_unit = if_else(fo_constr == 'netto', netto_12, kol_12),
         fo_unit_bp = if_else(fo_constr == 'netto', NETTO_bp, KOL_bp)
  ) %>%
  group_by(STRANA, NAPR, TNVED6, PERIOD) %>% # меняется группировка тут
  reframe(fizob6 = sum(fo_unit * share_TNVED6), # и доли тут
          price6 = sum(price_12 * share_TNVED6),
          bp = min(first_year_entry)
  ) %>%
  mutate(fizob6_bp = mean(fizob6[PERIOD >= bp & PERIOD <= bp %m+% months(11)], na.rm = TRUE),
         price6_bp = mean(price6[PERIOD >= bp & PERIOD <= bp %m+% months(11) & price6 > 0], na.rm = TRUE),
         .by = c(STRANA, NAPR, TNVED6) # !
  ) %>%
  mutate(fizob6 = fizob6 / fizob6_bp,
         price6 = price6 / price6_bp) #!

#-----------------------------------
# Физоб по странам -----------------
#-----------------------------------

fo_tot <- 
  data_fo %>%
  mutate(fo_unit = if_else(fo_constr == 'netto', netto_12, kol_12),
         fo_unit_bp = if_else(fo_constr == 'netto', NETTO_bp, KOL_bp)
  ) %>%
  # Группировка не включает TNVED — агрегат по всем кодам
  group_by(STRANA, NAPR, PERIOD) %>%
  reframe(fizob = sum(fo_unit),
          bp = min(first_year_entry)
  ) %>%
  mutate(fizob_bp = mean(fizob[PERIOD >= bp & PERIOD <= bp %m+% months(11)], na.rm = TRUE),
         .by = c(STRANA, NAPR)
  ) %>%
  mutate(fizob = fizob / fizob_bp,
         TNVED2 = 0L)  # маркер агрегата по всем кодам для fizob_index

###################
# Таблицы # ALL ###
###################

data_fo_all <- 
  df_complete %>%
  mutate(TNVED2 = substr(TNVED, start = 1, stop = 2),
         TNVED4 = substr(TNVED, start = 1, stop = 4),
         TNVED6 = substr(TNVED, start = 1, stop = 6)
  ) %>%
  arrange(STRANA, NAPR, TNVED, PERIOD) %>%
  group_by(NAPR, TNVED2, PERIOD) %>%
  mutate(share_TNVED2 = stoim_12 / sum(stoim_12, na.rm = T)) %>%
  ungroup() %>%
  group_by(NAPR, TNVED4, PERIOD) %>%
  mutate(share_TNVED4 = stoim_12 / sum(stoim_12, na.rm = T)) %>%
  ungroup() %>%
  group_by(NAPR, TNVED6, PERIOD) %>%
  mutate(share_TNVED6 = stoim_12 / sum(stoim_12, na.rm = T)) %>%
  ungroup() %>%
  mutate(across(
    c(share_TNVED2, share_TNVED4, share_TNVED6),
    ~ .x %>%
      coalesce(0) 
  )
  )

# 2 знак

fo_2_gr <-
  data_fo_all %>%
  group_by(NAPR, TNVED2, PERIOD) %>%
  reframe(fizob2 = sum(NETTO * share_TNVED2),
          price2 = sum(price_12 * share_TNVED2),
          bp = min(first_year_entry)
  ) %>%
  # Считаем значение физобъёма в базовый период. Тут группировка по трём признакам
  mutate(fizob2_bp = mean(fizob2[PERIOD >= bp & PERIOD <= bp %m+% months(11)], na.rm = TRUE),
         price2_bp = mean(price2[PERIOD >= bp & PERIOD <= bp %m+% months(11) & price2 > 0], na.rm = TRUE),
         .by = c(NAPR, TNVED2)
  ) %>%
  # Значения физобъёмов, делённые на среднее значения в базовый год. Тут группировка уже не нужна.
  mutate(fizob2 = fizob2 / fizob2_bp,
         price2 = price2 / price2_bp,
         STRANA = 'ALL'
  )

# 4 знак

fo_4_gr <-
  data_fo_all %>%
  group_by(NAPR, TNVED4, PERIOD) %>%
  reframe(fizob4 = sum(NETTO * share_TNVED4),
          price4 = sum(price_12 * share_TNVED4),
          bp = min(first_year_entry)
  ) %>%
  # Считаем значение физобъёма в базовый период. Тут группировка по трём признакам
  mutate(fizob4_bp = mean(fizob4[PERIOD >= bp & PERIOD <= bp %m+% months(11)], na.rm = TRUE),
         price4_bp = mean(price4[PERIOD >= bp & PERIOD <= bp %m+% months(11) & price4 > 0], na.rm = TRUE),
         .by = c(NAPR, TNVED4)
  ) %>%
  # Значения физобъёмов, делённые на среднее значения в базовый год. Тут группировка уже не нужна.
  mutate(fizob4 = fizob4 / fizob4_bp,
         price4 = price4 / price4_bp,
         STRANA = 'ALL'
  )

# 6 знак

fo_6_gr <-
  data_fo_all %>%
  group_by(NAPR, TNVED6, PERIOD) %>%
  reframe(fizob6 = sum(NETTO * share_TNVED6),
          price6 = sum(price_12 * share_TNVED6),
          bp = min(first_year_entry)
  ) %>%
  # Считаем значение физобъёма в базовый период. Тут группировка по трём признакам
  mutate(fizob6_bp = mean(fizob6[PERIOD >= bp & PERIOD <= bp %m+% months(11)], na.rm = TRUE),
         price6_bp = mean(price6[PERIOD >= bp & PERIOD <= bp %m+% months(11) & price6 > 0], na.rm = TRUE),
         .by = c(NAPR, TNVED6)
  ) %>%
  # Значения физобъёмов, делённые на среднее значения в базовый год. Тут группировка уже не нужна.
  mutate(fizob6 = fizob6 / fizob6_bp,
         price6 = price6 / price6_bp,
         STRANA = 'ALL'
  )


# Добавляем таблицы ALL как строки

fo_2 <- bind_rows(fo_2, fo_2_gr)
fo_4 <- bind_rows(fo_4, fo_4_gr)
fo_6 <- bind_rows(fo_6, fo_6_gr)

#------------------------------------------
# Сохранение результатов в parquet файлах -
#------------------------------------------

dbDisconnect(con, shutdown = TRUE) # Закрываем соединение

fo_2 %>% arrow::write_parquet('~/MGIMO-FT/data_processed/fizob_2.parquet') # Итоговые файлы в формате parquet.
fo_4 %>% arrow::write_parquet('~/MGIMO-FT/data_processed/fizob_4.parquet')
fo_6 %>% arrow::write_parquet('~/MGIMO-FT/data_processed/fizob_6.parquet')
fo_tot %>% arrow::write_parquet('~/MGIMO-FT/data_processed/fizob_total.parquet')