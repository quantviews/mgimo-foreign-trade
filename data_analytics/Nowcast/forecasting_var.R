library(tidyverse)
library(duckdb)
library(dfms)

con <- dbConnect(
  duckdb::duckdb(),
  "~/MGIMO-FT/db/unified_trade_data.duckdb",
  read_only = TRUE
)

dbGetQuery(con, "SHOW TABLES")

# В самом простом случае мы хотим оценить модель по общей сумме для верхнего кода, а потом размазать для более низких кодов и по странам.
# Много минусов, мы не учитываем заменяемость при импорте например.
# Зато это будет быстрее всего и можно посмотреть, насколько разумный получается результат.
# Я использую библиотеку dfms: https://cran.r-project.org/web/packages/dfms/vignettes/introduction.html

# План такой:
# Тренировочная и тестоая выборки.
# Пусть тестовая будет весь 2025 год.

df_raw <- dbGetQuery(con, "
  SELECT PERIOD, TNVED2, NAPR, STOIM
  FROM unified_trade_data"
                     ) %>%
  reframe(stoim = sum(STOIM, na.rm = T),
          .by = c('PERIOD', 'TNVED2', 'NAPR') 
          ) %>%
  arrange(TNVED2, NAPR, PERIOD) %>%
  mutate(gr = paste0(TNVED2, '_', NAPR))

df_var_1 <- df_raw %>%
  mutate(stoim = c(0, diff(log1p(stoim))) %>%
           as.numeric(),
         .by = 'gr') %>%
  filter(PERIOD > as_date('2019-01-01')) %>%
  select(PERIOD, gr, stoim) 

# 2 выборки

train_dates <- seq(from = df_var_1 %>% pull(PERIOD) %>% first(),
                   to = ymd('2024-12-01'),
                   by = 'month'
)
test_dates <- seq(from = ymd('2025-01-01'),
                  to = df_var_1 %>% pull(PERIOD) %>% last(),
                  by = 'month'
)

df_var_1 %>%
  ggplot2::ggplot(ggplot2::aes(x = PERIOD, y = stoim, color = gr)) +
  ggplot2::geom_line(show.legend = FALSE)

df_var_1_train <- 
  df_var_1 %>%
  pivot_wider(names_from = gr, values_from = stoim) %>%
  filter(PERIOD %in% train_dates) %>%
  select(-PERIOD)

# Диагностика для DFM

ic <- ICr(df_var_1_train)
plot(ic)
screeplot(ic) # Мне кажется, 6-7 ок.
n_var_lags <- vars::VARselect(ic$F_pca[, 1:7]) # 9 лагов в VAR # !

# Оценка модели. Я относительно гибко настроил выбор кол-ва параметров для оценки:

model_1 <- DFM(df_var_1_train,
               r = ic$r.star[3],
               p = n_var_lags$selection[1]
               )

# Работает достаточно быстро, оценка занимает около 5 секунд.

# Диагностика факторов

plot(model_1, method = "all", type = "individual")

# Оценённые значения (fitted)

fitted(model_1, orig.format = TRUE) %>%
  mutate(PERIOD = train_dates) %>%
  pivot_longer(-PERIOD) %>%
  ggplot(aes(x = PERIOD, y = value, color = name)) +
  geom_line(show.legend = F)
  
# Прогноз

forecast_test <- predict(model_1, h = length(test_dates))
forecast_test <- forecast_test$X_fcst %>%
  as_tibble(.name_repair = 'minimal') %>%
  mutate(PERIOD = test_dates,
         type = 'pred') %>%
  pivot_longer(-c(PERIOD, type),
               names_to = 'gr',
               values_to = 'stoim')

df_var_1 %>%
  filter(PERIOD %in% test_dates) %>%
  mutate(type = 'fact') %>%
  bind_rows(forecast_test) %>%
  filter(str_starts(gr, '6')) %>% # здесь можно выбрать группы, для которых мы хотим показать результаты
  ggplot(aes(x = PERIOD, y = stoim, color = type)) +
  geom_line() +
  facet_wrap(~ gr)

# 

# Возвращаем в исходный вид

forecast_level <- 
  forecast_test %>%
  left_join(
    df_raw %>%
      filter(PERIOD == last(train_dates)) %>%
      select(gr, stoim_last = stoim),
  by = 'gr'
  ) %>%
  arrange(gr, PERIOD) %>%
  mutate(
    stoim_level = expm1(log1p(stoim_last) + cumsum(stoim))
  ) %>%
  mutate(TNVED2 = substr(gr, start = 1, stop = 2),
         NAPR = substr(gr, start = 4, stop = 5)
         ) %>%
  select(PERIOD, TNVED2, NAPR, STOIM = stoim_level, type)

# Южная Корея - окт. её доля в коде 60 ИМ в окт. = 0.1, Х
# Прогноз_1 = Прогноз - СУММА(ФАКТ, Дек)
# ЮК 60 ИМ в дек = Прогноз в дек * Х

# Теперь алгоритм следующий:
# Тут есть существенные тонкости!
# Нужно разбить сырые данные по странам и периодам и посмотреть, есть ли значения.
# Также нужно вычесть из прогноза уже реализовавшиеся значения!
# Потом нужно получить доли страны i в низкоуровневом коде j (за последние 12 мес) и умножить прогноз на эту цифру.