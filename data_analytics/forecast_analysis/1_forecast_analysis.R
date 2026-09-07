library(tidyverse)
library(duckdb)
library(dfms)
library(microbenchmark)

source("data_analytics/forecast_analysis/functions/functions_forecasts.R")

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

# Длинный формат

df_wide <- df_var_1 %>%
   pivot_wider(
      names_from = gr,
      values_from = stoim
   )

# Интервалы для тренировки и прогнозов

# ============================================================
# 2. FORECAST EXPERIMENTS
# ============================================================

experiments <- tribble(
   ~experiment,      ~train_from,              ~train_to,               ~test_from,            ~test_to,
   "year_2025",      as_date("2019-02-01"),    as_date("2024-12-01"),   as_date("2025-01-01"), as_date("2025-12-01"),
   "q4_2025",        as_date("2019-02-01"),    as_date("2025-09-01"),   as_date("2025-10-01"), as_date("2025-12-01"),
   "q1_2026",        as_date("2019-02-01"),    as_date("2025-12-01"),   as_date("2026-01-01"), as_date("2026-03-01")
)

#################

model_specs <- list(
   static = list(),
   naive  = list(),
   snaive = list(period = 12),
   ar     = list(max_p = 12),
   covid  = list(max_p = 12, start = as_date("2019-02-01")),
   ma     = list(max_q = 12),
   arima  = list(),
   sarima = list(max_p = 2, max_q = 2, max_P = 1, max_Q = 1),
   ets    = list(),
   var    = list(max_p = 6),
   dfm    = list(max_p = 6, max_p_final = 2),
   dfm_stl = list(max_p = 6, max_p_final = 2),
   dfm_ets = list(max_p = 6, max_p_final = 2),
   bvar   = list(max_p = 6),
   fadreg = list()
)

forecast_results <- map_dfr(
   experiments$experiment,
   ~ run_experiment(
      data = df_wide,
      experiment_name = .x,
      model_specs = model_specs,
      experiments = experiments
   )
)

# Equal-weight average of SARIMA and ETS+DFM (no extra fit)
forecast_results <- bind_rows(
   forecast_results,
   forecast_results %>%
      filter(model %in% c("sarima", "dfm_ets")) %>%
      pivot_wider(names_from = model, values_from = forecast) %>%
      mutate(
         forecast = 0.5 * sarima + 0.5 * dfm_ets,
         model = "avg_sarima_dfm_ets"
      ) %>%
      select(PERIOD, gr, forecast, experiment, model)
)

# тест на полноту:
# n - число периодов x кол-во рядов.

forecast_results %>%
   count(experiment, model) # верно

# объединяем с фактическими данными

forecast_results_u <- forecast_results %>%
   left_join(
      df_var_1 %>%
         select(PERIOD, gr, actual = stoim),
      by = c("PERIOD", "gr")
   ) %>%
   group_by(experiment, model, gr) %>%
   arrange(PERIOD, .by_group = TRUE) %>%
   mutate(horizon = row_number()) %>%
   ungroup()

forecast_results_u %>%
   summarise(
      n = n(),
      n_missing_forecast = sum(is.na(forecast)),
      n_missing_actual = sum(is.na(actual))
   )

accuracy_results <- forecast_results_u %>%
   mutate(
      error = forecast - actual,
      abs_error = abs(error),
      sq_error = error^2
   ) %>%
   group_by(experiment, model) %>%
   summarise(
      MAE = mean(abs_error, na.rm = TRUE),
      RMSE = sqrt(mean(sq_error, na.rm = TRUE)),
      .groups = "drop"
   ) %>%
   arrange(experiment, RMSE)

table_wmae <- 
   forecast_results_u %>%
   reframe(
      mean_mae = mean(abs(actual - forecast)),
      .by = c(gr, experiment, model)
   ) %>%
   left_join(
      df_raw %>%
         filter(
            PERIOD >= ymd("2025-01-01"),
            PERIOD <= ymd("2025-12-01")
         ) %>%
         reframe(
            total_stoim = sum(stoim) / 12,
            .by = gr
         ),
      by = "gr"
   ) %>%
   reframe(
      WMAE = weighted.mean(mean_mae, total_stoim),
      .by = c(model, experiment)
   ) %>%
   arrange(experiment, WMAE)

# Бенчмарки скорости

benchmark_results <- crossing(
   experiment = "q4_2025",
   model = names(model_specs)
) %>%
   pmap_dfr(
      function(experiment, model) {
         
         message("Benchmark: ", experiment, " | ", model)
         
         benchmark_model(
            data = df_wide,
            experiment_name = experiment,
            method = model,
            model_specs = model_specs,
            experiments = experiments %>%
               filter(.data$experiment == experiment),
            times = 5
         )
      }
   )

# Ensemble time ≈ sum of components (average itself is O(hk))
benchmark_results <- bind_rows(
   benchmark_results,
   benchmark_results %>%
      filter(model %in% c("sarima", "dfm_ets")) %>%
      summarise(
         across(c(median_sec, mean_sec, min_sec), sum),
         .by = experiment
      ) %>%
      mutate(model = "avg_sarima_dfm_ets")
)

benchmark_results %>%
   arrange(experiment, median_sec)

results <- accuracy_results %>%
   left_join(
      benchmark_results %>%
         select(experiment, model, median_sec),
      by = c("experiment", "model")
   ) %>%
   left_join(
      table_wmae,
      by = c('model', 'experiment')
   )

model_labels <- c(
   static = "Static",
   naive  = "Naive",
   snaive = "Seasonal naive",
   ar     = "AR",
   covid  = "AR + COVID",
   ma     = "MA",
   arima  = "ARIMA",
   sarima = "SARIMA",
   ets    = "ETS",
   var    = "PCA + VAR",
   dfm    = "DFM",
   dfm_stl = "STL + DFM",
   dfm_ets = "ETS + DFM",
   avg_sarima_dfm_ets = "Avg(SARIMA, ETS+DFM)",
   bvar   = "PCA + BVAR",
   fadreg = "FADREG"
)

final_table <- results %>%
   mutate(
      model = recode(model, !!!model_labels)
   ) %>%
   select(
      experiment,
      model,
      MAE,
      RMSE,
      WMAE,
      Time_sec = median_sec
   ) %>%
   mutate(
      across(
         c(MAE, RMSE, WMAE, Time_sec),
         ~ round(.x, 3)
      )
   ) %>%
   arrange(experiment, MAE) %>%
   mutate(
      experiment = recode(
         experiment,
         year_2025 = "2025",
         q4_2025 = "Q4 2025",
         q1_2026 = "Q1 2026"
      )
   ) %>%
   pivot_wider(
      names_from = experiment,
      values_from = c(MAE, RMSE, WMAE, Time_sec),
      names_glue = "{.value}_{experiment}"
   ) %>%
   arrange(MAE_2025)

final_table %>%
   select(-c(`Time_sec_Q1 2026`, `Time_sec_2025`),
          starts_with('RMSE')) %>%
   rename(Time_sec = `Time_sec_Q4 2025`) %>%
   writexl::write_xlsx('data_analytics/forecast_analysis/results/results_table.xlsx')