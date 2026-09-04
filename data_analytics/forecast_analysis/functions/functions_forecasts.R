source('data_analytics/forecast_analysis/functions/naive.R')
source('data_analytics/forecast_analysis/functions/static.R')
source('data_analytics/forecast_analysis/functions/ar.R')
source('data_analytics/forecast_analysis/functions/ma.R')
source('data_analytics/forecast_analysis/functions/arima.R')
source('data_analytics/forecast_analysis/functions/var.R')
source('data_analytics/forecast_analysis/functions/dfm.R')
source('data_analytics/forecast_analysis/functions/bvar.R') #*
source('data_analytics/forecast_analysis/functions/fadreg.R')

#######################

fit_model <- function(data, method, specs) {
   
   args <- c(
      list(data = data),
      specs[[method]]
   )
   
   do.call(
      paste0("fit_", method),
      args
   )
}

# forecast

forecast_model <- function(model, method, h) {
   
   do.call(
      paste0("forecast_", method),
      list(model = model, h = h)
   )
}

# run experiment

run_experiment <- function(data, experiment_name, model_specs, experiments) {
   
   exp <- experiments %>%
      filter(experiment == experiment_name)
   
   train <- data %>%
      filter(
         PERIOD >= exp$train_from,
         PERIOD <= exp$train_to
      )
   
   test <- data %>%
      filter(
         PERIOD >= exp$test_from,
         PERIOD <= exp$test_to
      )
   
   x_train <- train %>%
      select(-PERIOD)
   
   h <- nrow(test)
   
   map_dfr(names(model_specs), function(method) {
      
      message(experiment_name, " | ", method)
      
      model <- fit_model(
         data = x_train,
         method = method,
         specs = model_specs
      )
      
      fc <- forecast_model(
         model = model,
         method = method,
         h = h
      )
      
      as_tibble(fc, .name_repair = "minimal") %>%
         set_names(names(x_train)) %>%
         mutate(PERIOD = test$PERIOD) %>%
         pivot_longer(
            cols = -PERIOD,
            names_to = "gr",
            values_to = "forecast"
         ) %>%
         mutate(
            experiment = experiment_name,
            model = method
         )
   })
}

# microbenchmark

benchmark_model <- function(
      data,
      experiment_name,
      method,
      model_specs,
      experiments,
      times = 5
) {
   
   exp <- experiments %>%
      filter(experiment == experiment_name)
   
   train <- data %>%
      filter(
         PERIOD >= exp$train_from,
         PERIOD <= exp$train_to
      )
   
   x_train <- train %>%
      select(-PERIOD)
   
   h <- data %>%
      filter(
         PERIOD >= exp$test_from,
         PERIOD <= exp$test_to
      ) %>%
      nrow()
   
   bm <- microbenchmark::microbenchmark(
      fit_forecast = {
         
         model <- fit_model(
            data = x_train,
            method = method,
            specs = model_specs
         )
         
         forecast_model(
            model = model,
            method = method,
            h = h
         )
      },
      times = times
   )
   
   tibble(
      experiment = experiment_name,
      model = method,
      median_sec = median(bm$time) / 1e9,
      mean_sec = mean(bm$time) / 1e9,
      min_sec = min(bm$time) / 1e9
   )
}