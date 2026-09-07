library(tidyverse)
library(duckdb)
library(dfms)

con <- dbConnect(
  duckdb::duckdb(),
  "db/unified_trade_data.duckdb",
  read_only = TRUE
)

#dbDisconnect(con, shutdown = TRUE) # На всякий случай оставлю тут

dbGetQuery(con, "SHOW TABLES")

#-----------------------------------------
# Нужно определить:
# A) первую дату, с которой начинается прогноз (тут всё не совсем просто),
# Б) дату, до которой будет прогноз (тут всё просто),
# В) Кол-во периодов для прогноза (Б - А).
#-----------------------------------------

dbGetQuery(con, "
  SELECT PERIOD, TNVED2, NAPR, STOIM, STRANA, TYPE
  FROM unified_trade_data"
) %>%
  filter(TYPE == 'fact') %>%
  reframe(last_period = max(PERIOD),
          .by = c(STRANA)
          ) %>%
  filter(last_period > max(last_period) %m-% months(11)) %>%
  ggplot(aes(x = last_period)) +
  geom_histogram()

fc_dates  <- 
dbGetQuery(con, "
  SELECT PERIOD, TNVED2, NAPR, STOIM, STRANA, TYPE
  FROM unified_trade_data"
           ) %>%
  filter(TYPE == 'fact') %>%
  reframe(last_period = max(PERIOD),
          .by = c(STRANA)
  ) %>%
  filter(last_period > last(last_period) %m-% months(11)) %>%
  arrange(last_period) %>%
  mutate(last_period_cdf = cume_dist(last_period)) %>%
  filter(last_period_cdf >= 0.5) # Нужно обсудить
  
fc_from <- 
  fc_dates %>%
  pull(last_period) %>%
  min()
  
fc_to <- 
  fc_dates %>%
  pull(last_period) %>%
  max()

fc_periods <- interval(fc_from, fc_to) %/% months(1) + 1

# ----------------------------------------------
# Теперь мы хотим сделать прогноз на fc_periods
# ----------------------------------------------
  
# Табличка с сырыми данными. Она нам понадобится потом для join-а, поэтому пусть будет в памяти.

dbGetQuery(con, "
  SELECT PERIOD, TNVED2, NAPR, STOIM, STRANA, TYPE
  FROM unified_trade_data"
) %>%
   filter(TYPE == 'fact') %>%
   reframe(last_period = max(PERIOD),
           .by = c(STRANA)
   ) %>%
   filter(last_period > max(last_period) %m-% months(11)) %>%
   ggplot(aes(x = last_period)) +
   geom_histogram()

fc_dates  <- 
   dbGetQuery(con, "
  SELECT PERIOD, TNVED2, NAPR, STOIM, STRANA, TYPE
  FROM unified_trade_data"
   ) %>%
   filter(TYPE == 'fact') %>%
   reframe(last_period = max(PERIOD),
           .by = c(STRANA)
   ) %>%
   filter(last_period > last(last_period) %m-% months(11)) %>%
   arrange(last_period) %>%
   mutate(last_period_cdf = cume_dist(last_period)) %>%
   filter(last_period_cdf >= 0.5) # Нужно обсудить

fc_from <- 
   fc_dates %>%
   pull(last_period) %>%
   min()

fc_to <- 
   fc_dates %>%
   pull(last_period) %>%
   max()

fc_periods <- interval(fc_from, fc_to) %/% months(1) + 1

# ----------------------------------------------
# Теперь мы хотим сделать прогноз на fc_periods
# ----------------------------------------------

# Табличка с сырыми данными.
# Обновлённый вариант, исключает ошибки с пропусками данных для групп при моделировании

df_raw <- dbGetQuery(con, "
  SELECT PERIOD, TNVED2, NAPR, STOIM, TYPE
  FROM unified_trade_data
") %>%
   filter(TYPE == "fact") %>%
   reframe(
      stoim = sum(STOIM, na.rm = TRUE),
      .by = c("PERIOD", "TNVED2", "NAPR")
   )

periods <- seq(
   from = min(df_raw$PERIOD),
   to   = max(df_raw$PERIOD),
   by   = "month"
)

groups <- df_raw %>%
   distinct(TNVED2, NAPR)

df_raw <- groups %>%
   crossing(PERIOD = periods) %>% # Как cross_join только между df и вектором  
   left_join(
      df_raw,
      by = c("TNVED2", "NAPR", "PERIOD")
   ) %>%
   mutate(
      stoim = replace_na(stoim, 0),
      gr = paste0(TNVED2, "_", NAPR)
   ) %>%
   arrange(TNVED2, NAPR, PERIOD)

# Трансформация данных (log1p %>% diff).
# Почему log1p? Потому что у нас "могут быть" значения 0, а сами значения STOIM большие, поэтому прибавка 1 не искажает результат.

df_var_1 <- df_raw %>%
   mutate(stoim = c(0, diff(log1p(stoim))) %>%
             as.numeric(),
          .by = 'gr') %>%
   filter(PERIOD > as_date('2019-01-01')) %>%
   select(PERIOD, gr, stoim) 

# Картинка

df_var_1 %>%
  ggplot2::ggplot(ggplot2::aes(x = PERIOD, y = stoim, color = gr)) +
  ggplot2::geom_line(show.legend = FALSE)

# 2 Временных промежутка (не пересекающихся)

train_dates <- seq(from = df_var_1 %>% pull(PERIOD) %>% first(),
                   to = fc_from %m-% months(1),
                   by = 'month'
)

test_dates <- seq(from = fc_from,
                  to = fc_to,
                  by = 'month'
)

df_var_1_train <- 
  df_var_1 %>%
  pivot_wider(names_from = gr, values_from = stoim) %>%
  filter(PERIOD %in% train_dates) %>%
  select(-PERIOD)

# Оценка моделей

model_dfm <-
   fit_model(
   data = df_var_1_train,
   method = 'dfm_ets',
   specs = list(max_p = 6, max_p_final = 2)
)

model_sarima <-
   fit_model(
      data = df_var_1_train,
      method = 'sarima',
      specs = list(max_p = 2, max_q = 2, max_P = 1, max_Q = 1)
   )

# Прогноз

fc_dfm    <- forecast_model(model_dfm,    method = "dfm_ets", h = fc_periods)
fc_sarima <- forecast_model(model_sarima, method = "sarima",  h = fc_periods)
fc_avg    <- 0.5 * fc_dfm + 0.5 * fc_sarima

forecast_test <-
   fc_avg %>% 
   as_tibble(.name_repair = 'minimal') %>%
   mutate(PERIOD = test_dates,
          type = 'pred') %>%
   pivot_longer(-c(PERIOD, type),
                names_to = 'gr',
                values_to = 'stoim')

# Возвращаем в исходный вид

forecast_level <- 
  forecast_test %>%
  left_join(
    df_raw %>%
      filter(PERIOD == max(train_dates)) %>%
      select(gr, stoim_last = stoim),
    by = 'gr'
  ) %>%
  arrange(gr, PERIOD) %>%
  mutate(
    stoim_level = expm1(log1p(stoim_last) + cumsum(stoim)),
    .by = gr
  ) %>%
  mutate(TNVED2 = substr(gr, start = 1, stop = 2),
         NAPR = substr(gr, start = 4, stop = 5)
  ) %>%
  select(PERIOD, TNVED2, NAPR, STOIM = stoim_level, type)

# Итоговая табличка с исходными данными и прогнозами.
# Её мы будем объединять с табличками на следующих шагах.

res_2 <- 
  df_raw %>%
  select(PERIOD, TNVED2, NAPR, STOIM = stoim) %>%
  filter(PERIOD < fc_from) %>%
  mutate(type = 'fact') %>%
  bind_rows(forecast_level)

res_2 %>% 
  mutate(gr = paste0(TNVED2, '_', NAPR)) %>%
  filter(str_starts(TNVED2, '70')) %>%
  ggplot(aes(x = PERIOD, y = STOIM, color = type)) +
  geom_line() +
  facet_wrap(~gr, scales = 'free')

#--------------------------------------------------------
# Раскладываем через веса для  10 уровня ----------------
#--------------------------------------------------------

df_10 <- 
  # БД для STOIM и NETTO на 10 коде
  dbGetQuery(con, "
  SELECT PERIOD, STRANA, TNVED2, TNVED, NAPR, STOIM, NETTO, TYPE
  FROM unified_trade_data"
  ) %>%
  filter(TYPE == 'fact') %>%
  filter(any(STOIM > 0), .by = c(STRANA, NAPR, TNVED)) %>% # Здесь я фильтровал базу данных, чтобы убрать группы, для которых все данные 0. Для Индии.
  mutate(PERIOD = as_date(PERIOD)) %>% # Перевожу в date
  # Заполняю пропуски без группировки - замена для group_by %>% complete. Минут на 5 быстрее
  right_join(
    dbGetQuery(con, "
  SELECT STRANA, NAPR, TNVED, PERIOD, STOIM, NETTO, TYPE
  FROM unified_trade_data
               ") %>%
      filter(TYPE == 'fact') %>%
      filter(any(STOIM > 0), .by = c(STRANA, NAPR, TNVED)) %>%
      distinct(STRANA, TNVED, NAPR) %>%
      cross_join(
        data.frame(
          PERIOD = seq.Date(
            min(df_raw$PERIOD), 
            max(test_dates), by = "month")
        )
      ),
    by = c('STRANA', 'NAPR', 'TNVED', 'PERIOD')
  ) %>%
  # Если STOIM == NA -> 0
  mutate(
    STOIM = coalesce(STOIM, 0),
    NETTO = coalesce(NETTO, 0)
  ) %>%
  mutate(TNVED2 = substr(TNVED, start = 1,stop = 2))  %>%
  arrange(STRANA, TNVED, NAPR, PERIOD) %>%
  # Join с таблицей прогнозов!
  #mutate(stoim_2 = sum(STOIM, na.rm = T),
  #       .by = c('PERIOD', 'STRANA', 'TNVED2', 'NAPR')
  #) %>%
  full_join(
    res_2 %>%
      select(PERIOD, TNVED2, NAPR, type, STOIM_ALL_2 = STOIM),
    by = c('PERIOD', 'TNVED2', 'NAPR')
  ) %>%
  # Конструирование весов
  mutate(share = STOIM / STOIM_ALL_2) %>%
  mutate(
    share_mean = mean(share[PERIOD %in% tail(train_dates, 12)], na.rm = TRUE),
    price_mean = mean(STOIM[PERIOD %in% tail(train_dates, 12)] / NETTO[PERIOD %in% tail(train_dates, 12)], na.rm = TRUE),
    .by = c('TNVED', 'STRANA', 'NAPR')
  ) %>%
  mutate(
    share_mean = if_else(
      type == "pred",
      share_mean[type == "fact"][1],
      share_mean
    ),
    price_mean = if_else(
      type == "pred",
      price_mean[type == "fact"][1],
      price_mean
    ),
    .by = c("TNVED", "STRANA", "NAPR")
  ) %>%
  # Разложение стоимости по весам
  mutate(
    stoim_fc = if_else(type == 'pred', STOIM_ALL_2 * share_mean, STOIM_ALL_2 * share),
    netto_fc = if_else(type == 'pred', stoim_fc / price_mean, STOIM_ALL_2 / price_mean)
  )

df_10_tidy <-
  df_10 %>%
  mutate(netto_fc = pmax(NETTO, netto_fc, na.rm = T)) %>%
  select(STRANA,
         PERIOD,
         TNVED,
         NAPR,
         TYPE = type,
         STOIM = stoim_fc,
         NETTO = netto_fc
  ) %>%
  filter(!is.na(TYPE))

df_10_complementary <-
  dbGetQuery(con, "
  SELECT PERIOD, STRANA, TNVED, NAPR, STOIM, NETTO, TYPE
  FROM unified_trade_data"
  ) %>%
  filter(TYPE == 'fact') %>%
  filter(PERIOD >= first(fc_from)) %>%
  arrange(STRANA, TNVED, NAPR, PERIOD) %>%
  mutate(TYPE = 'fact')

# Сохранение результатов

df_10_tidy %>%
  anti_join(
    df_10_complementary,
    by = c("STRANA", "NAPR", "TNVED", "PERIOD")
  ) %>%
  bind_rows(df_10_complementary) %>%
  write_parquet('data_processed/nowcast_new.parquet')
