#-----------------------------------------------------------------
# Теперь мы должны построить табличку с весами на 4 и 6 уровнях --
#-----------------------------------------------------------------

df_4 <- 
  dbGetQuery(con, "
  SELECT PERIOD, STRANA, TNVED2, TNVED4, NAPR, STOIM, NETTO
  FROM unified_trade_data"
  ) %>%
  filter(any(STOIM > 0), .by = c(STRANA, NAPR, TNVED4)) %>% # Здесь я фильтровал базу данных, чтобы убрать группы, для которых все данные 0. Для Индии.
  mutate(PERIOD = as_date(PERIOD)) %>% # Перевожу в date
  # Обобщаю стоимость
  reframe(STOIM = sum(STOIM, na.rm = T),
          NETTO = sum(NETTO, na.rm = T),
          .by = c('STRANA', 'PERIOD', 'TNVED4', 'NAPR') 
  )  %>%
  # Заполняю пропуски без группировки - замена для group_by %>% complete. Минут на 5 быстрее
  right_join(
    dbGetQuery(con, "
  SELECT STRANA, NAPR, TNVED4, PERIOD, STOIM, NETTO
  FROM unified_trade_data
               ") %>%
      filter(any(STOIM > 0), .by = c(STRANA, NAPR, TNVED4)) %>%
      distinct(STRANA, TNVED4, NAPR) %>%
      cross_join(
        data.frame(
          PERIOD = seq.Date(
            min(df_raw$PERIOD), 
            max(test_dates), by = "month")
        )
      ),
    by = c('STRANA', 'NAPR', 'TNVED4', 'PERIOD')
  ) %>%
  mutate(
    STOIM = coalesce(STOIM, 0),
    NETTO = coalesce(NETTO, 0)
  ) %>%
  mutate(TNVED2 = substr(TNVED4, start = 1,stop = 2))  %>%
  arrange(STRANA, TNVED4, NAPR, PERIOD) %>%
  # Join с таблицей прогнозов
  mutate(stoim_2 = sum(STOIM, na.rm = T),
         .by = c('PERIOD', 'STRANA', 'TNVED2', 'NAPR')
  ) %>%
  full_join(
    res_2 %>%
      select(PERIOD, TNVED2, NAPR, type, STOIM_ALL_2 = STOIM),
    by = c('PERIOD', 'TNVED2', 'NAPR')
  ) %>%
  # Конструирование весов
  mutate(share = stoim_2 / STOIM_ALL_2) %>%
  mutate(
    share_mean = mean(share[PERIOD %in% tail(train_dates, 12)], na.rm = TRUE),
    price_mean = mean(STOIM[PERIOD %in% tail(train_dates, 12)] / NETTO[PERIOD %in% tail(train_dates, 12)], na.rm = TRUE),
    .by = c('TNVED4', 'STRANA', 'NAPR')
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
    .by = c("TNVED4", "STRANA", "NAPR")
  ) %>%
  # Разложение стоимости по весам
  mutate(
    stoim_fc = if_else(type == 'pred', STOIM_ALL_2 * share_mean, STOIM_ALL_2 * share),
    netto_fc = if_else(type == 'pred', stoim_fc / price_mean, STOIM_ALL_2 / price_mean)
  )

df_4_tidy <-
  df_4 %>%
  mutate(netto_fc = pmax(NETTO, netto_fc, na.rm = T)) %>%
  select(STRANA,
         PERIOD,
         TNVED4,
         NAPR,
         TYPE = type,
         STOIM = stoim_fc,
         NETTO = netto_fc
  )

df_4_complementary <-
  dbGetQuery(con, "
  SELECT PERIOD, STRANA, TNVED4, NAPR, STOIM, NETTO
  FROM unified_trade_data"
  ) %>%
  filter(PERIOD >= first(fc_from)) %>%
  reframe(STOIM = sum(STOIM, na.rm = T),
          NETTO = sum(NETTO, na.rm = T),
          .by = c('STRANA', 'PERIOD', 'TNVED4', 'NAPR') 
  ) %>%
  arrange(STRANA, TNVED4, NAPR, PERIOD) %>%
  mutate(TYPE = 'fact')

bind_rows(df_4_tidy, df_4_complementary)  

write_parquet(bind_rows(df_4_tidy, df_4_complementary),
              '~/MGIMO-FT/data_processed/nowcast_4.parquet')

#--------------------------------------------------------
# То же самое для 6 уровня ------------------------------
#--------------------------------------------------------

df_6 <- 
  dbGetQuery(con, "
  SELECT PERIOD, STRANA, TNVED2, TNVED6, NAPR, STOIM, NETTO
  FROM unified_trade_data"
  ) %>%
  filter(any(STOIM > 0), .by = c(STRANA, NAPR, TNVED6)) %>% # Здесь я фильтровал базу данных, чтобы убрать группы, для которых все данные 0. Для Индии.
  mutate(PERIOD = as_date(PERIOD)) %>% # Перевожу в date
  # Обобщаю стоимость
  reframe(STOIM = sum(STOIM, na.rm = T),
          NETTO = sum(NETTO, na.rm = T),
          .by = c('STRANA', 'PERIOD', 'TNVED6', 'NAPR') 
  )  %>%
  # Заполняю пропуски без группировки - замена для group_by %>% complete. Минут на 5 быстрее
  right_join(
    dbGetQuery(con, "
  SELECT STRANA, NAPR, TNVED6, PERIOD, STOIM, NETTO
  FROM unified_trade_data
               ") %>%
      filter(any(STOIM > 0), .by = c(STRANA, NAPR, TNVED6)) %>%
      distinct(STRANA, TNVED6, NAPR) %>%
      cross_join(
        data.frame(
          PERIOD = seq.Date(
            min(df_raw$PERIOD), 
            max(test_dates), by = "month")
        )
      ),
    by = c('STRANA', 'NAPR', 'TNVED6', 'PERIOD')
  ) %>%
  mutate(
    STOIM = coalesce(STOIM, 0),
    NETTO = coalesce(NETTO, 0)
  ) %>%
  mutate(TNVED2 = substr(TNVED6, start = 1,stop = 2))  %>%
  arrange(STRANA, TNVED6, NAPR, PERIOD) %>%
  # Join с таблицей прогнозов
  mutate(stoim_2 = sum(STOIM, na.rm = T),
         .by = c('PERIOD', 'STRANA', 'TNVED2', 'NAPR')
  ) %>%
  full_join(
    res_2 %>%
      select(PERIOD, TNVED2, NAPR, type, STOIM_ALL_2 = STOIM),
    by = c('PERIOD', 'TNVED2', 'NAPR')
  ) %>%
  # Конструирование весов
  mutate(share = stoim_2 / STOIM_ALL_2) %>%
  mutate(
    share_mean = mean(share[PERIOD %in% tail(train_dates, 12)], na.rm = TRUE),
    price_mean = mean(STOIM[PERIOD %in% tail(train_dates, 12)] / NETTO[PERIOD %in% tail(train_dates, 12)], na.rm = TRUE),
    .by = c('TNVED6', 'STRANA', 'NAPR')
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
    .by = c("TNVED6", "STRANA", "NAPR")
  ) %>%
  # Разложение стоимости по весам
  mutate(
    stoim_fc = if_else(type == 'pred', STOIM_ALL_2 * share_mean, STOIM_ALL_2 * share),
    netto_fc = if_else(type == 'pred', stoim_fc / price_mean, STOIM_ALL_2 / price_mean)
  )

df_6_tidy <-
  df_6 %>%
  mutate(netto_fc = pmax(NETTO, netto_fc, na.rm = T)) %>%
  select(STRANA,
         PERIOD,
         TNVED6,
         NAPR,
         TYPE = type,
         STOIM = stoim_fc,
         NETTO = netto_fc
  )

df_6_complementary <-
  dbGetQuery(con, "
  SELECT PERIOD, STRANA, TNVED6, NAPR, STOIM, NETTO
  FROM unified_trade_data"
  ) %>%
  filter(PERIOD >= first(fc_from)) %>%
  reframe(STOIM = sum(STOIM, na.rm = T),
          NETTO = sum(NETTO, na.rm = T),
          .by = c('STRANA', 'PERIOD', 'TNVED6', 'NAPR') 
  ) %>%
  arrange(STRANA, TNVED6, NAPR, PERIOD) %>%
  mutate(TYPE = 'fact')

bind_rows(df_6_tidy, df_6_complementary)  

write_parquet(bind_rows(df_6_tidy, df_6_complementary),
              '~/MGIMO-FT/data_processed/nowcast_6.parquet')

#--------------------------------------------------------
# То же самое для 10 уровня -----------------------------
#--------------------------------------------------------