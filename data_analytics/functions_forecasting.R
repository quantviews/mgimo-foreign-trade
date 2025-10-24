forward_fill <- function(v) {
  result <- numeric(length(v))
  last_val <- NaN
  
  for (i in seq_along(v)) {
    if (is.na(v[i])) {
      if (is.nan(last_val)) {
        stop("Missing value at the start with no previous value to fill")
      }
      result[i] <- last_val
    } else {
      last_val <- v[i]
      result[i] <- v[i]
    }
  }
  
  return(result)
}

sarima_fun <- function(y, h){
  ts_y <- ts(y, frequency = 12)
  # Fit seasonal auto.arima model
  fit <- auto.arima(ts_y, seasonal = TRUE)
  
  # Forecast next 6 periods
  forecast(fit, h)$mean %>% c()
}

tbats_fun <- function(y, h) {
  ts_y <- ts(y, frequency = 12)
  fit <- tbats(ts_y)
  forecast(fit, h)$mean %>% c()
}

stl_ets_fun <- function(y, h) {
  ts_y <- ts(y, frequency = 12)
  fit <- stlf(ts_y, h = h, method = "ets")
  fit$mean %>% c()
}


evaluate_forecasts <- function(actual, forecasts) {
  # actual and forecasts are vectors of equal length
  list(
    rmse = rmse(actual, forecasts),
    mae = mae(actual, forecasts)
  )
}

run_forecasting_benchmark_not_parallel <- function(con, steps_ahead = 6, n_samples = 10, seed = 123) {
  set.seed(seed)
  
  # Get distinct TNVED codes from your data subset
  all_tnved <- dbGetQuery(con, "
    SELECT DISTINCT TNVED
    FROM unified_trade_data
    WHERE STRANA = 'CN' AND NAPR = 'ИМ'
  ")$TNVED
  
  # Sample TNVED codes randomly
  sampled_tnved <- sample(all_tnved, n_samples, replace = FALSE)
  
  results <- list()
  
  for (tnved in sampled_tnved) {
    # Pull time series data for this tnved
    df <- dbGetQuery(con, sprintf("
      SELECT TNVED, PERIOD, STOIM
      FROM unified_trade_data
      WHERE STRANA = 'CN' AND NAPR = 'ИМ' AND TNVED = '%s'
    ", tnved))
    
    df <- df %>% mutate(PERIOD = as.Date(PERIOD))
    
    # Fill missing periods with NA
    full_periods <- tibble(
      PERIOD = seq.Date(from = min(df$PERIOD), to = max(df$PERIOD), by = "month"),
      TNVED = tnved
    )
    
    df_1 <- full_periods %>%
      left_join(df, by = c("PERIOD", "TNVED")) %>%
      arrange(PERIOD) %>%
      mutate(STOIM_log = log1p(STOIM) %>% forward_fill())
    
    # Filter df_1 to have at least steps_ahead periods after training window
    # Let's say we use all except last steps_ahead as training
    if (nrow(df_1) < steps_ahead + 12) { # require minimum training length e.g. 12
      message(sprintf("Skipping TNVED %s: insufficient data", tnved))
      next
    }
    
    train_idx <- 1:(nrow(df_1) - steps_ahead)
    test_idx <- (nrow(df_1) - steps_ahead + 1):nrow(df_1)
    
    train_series <- df_1$STOIM_log[train_idx]
    actual_future <- df_1$STOIM_log[test_idx]
    
    # Run forecasting models
    if (length(train_series) < 24) {
      message(sprintf("Skipping TNVED %s: series too short (%d)", tnved, length(train_series)))
      next
    }
    
    fc_1 <- tryCatch(sarima_fun(train_series, steps_ahead), error = function(e) {
      message(sprintf("SARIMA failed for TNVED %s: %s", tnved, e$message))
      rep(NA_real_, steps_ahead)
    })
    
    fc_2 <- tryCatch(tbats_fun(train_series, steps_ahead), error = function(e) {
      message(sprintf("TBATS failed for TNVED %s: %s", tnved, e$message))
      rep(NA_real_, steps_ahead)
    })
    
    fc_3 <- tryCatch(stl_ets_fun(train_series, steps_ahead), error = function(e) {
      message(sprintf("STL-ETS failed for TNVED %s: %s", tnved, e$message))
      rep(NA_real_, steps_ahead)
    })
    
    # Evaluate errors
    err_1 <- evaluate_forecasts(actual_future, fc_1)
    err_2 <- evaluate_forecasts(actual_future, fc_2)
    err_3 <- evaluate_forecasts(actual_future, fc_3)
    
    # Store results
    results[[tnved]] <- list(
      tnved = tnved,
      errors = list(
        sarima = err_1,
        tbats = err_2,
        stl_ets = err_3
      )
    )
  }
  
  # Convert results to a tidy data frame
  results_df <- bind_rows(lapply(results, function(x) {
    tibble(
      TNVED = x$tnved,
      model = c("sarima", "tbats", "stl_ets"),
      RMSE = c(x$errors$sarima$rmse, x$errors$tbats$rmse, x$errors$stl_ets$rmse),
      MAE = c(x$errors$sarima$mae, x$errors$tbats$mae, x$errors$stl_ets$mae)
    )
  }))
  
  return(results_df)
}

run_forecasting_benchmark <- function(con,
                                      steps_ahead = 6,
                                      n_samples = 10,
                                      n_cores = parallel::detectCores() - 1,
                                      seed = 123) {
  library(DBI)
  library(dplyr)
  library(furrr)
  library(tibble)
  library(purrr)
  
  set.seed(seed)
  
  #---------------------------------------------------------------------------
  # 1️⃣ Load all relevant data once (in main process)
  #---------------------------------------------------------------------------
  message("Loading data from DuckDB...")
  df_all <- dbGetQuery(con, "
    SELECT TNVED, PERIOD, STOIM
    FROM unified_trade_data
    WHERE STRANA = 'CN' AND NAPR = 'ИМ'
  ")
  
  df_all <- df_all %>% mutate(PERIOD = as.Date(PERIOD))
  
  # Split by TNVED into list of data frames
  data_by_tnved <- split(df_all, df_all$TNVED)
  all_tnved <- names(data_by_tnved)
  
  # Randomly sample TNVED codes
  sampled_tnved <- sample(all_tnved, n_samples, replace = FALSE)
  message(sprintf("Sampled %d TNVED codes", length(sampled_tnved)))
  
  #---------------------------------------------------------------------------
  # 2️⃣ Set up parallel plan
  #---------------------------------------------------------------------------
  plan(multisession, workers = n_cores)
  message(sprintf("Running with %d parallel workers", n_cores))
  
  #---------------------------------------------------------------------------
  # 3️⃣ Define worker logic (applied in parallel)
  #---------------------------------------------------------------------------
  results <- future_map(sampled_tnved, function(tnved) {
    df <- data_by_tnved[[tnved]]
    
    # Fill missing months
    full_periods <- tibble(
      PERIOD = seq.Date(from = min(df$PERIOD), to = max(df$PERIOD), by = "month"),
      TNVED = tnved
    )
    
    df_1 <- full_periods %>%
      left_join(df, by = c("PERIOD", "TNVED")) %>%
      arrange(PERIOD) %>%
      mutate(STOIM_log = log1p(STOIM) %>% forward_fill())
    
    # Skip too-short series
    if (nrow(df_1) < steps_ahead + 12) {
      message(sprintf("Skipping TNVED %s: insufficient data", tnved))
      return(NULL)
    }
    
    train_idx <- 1:(nrow(df_1) - steps_ahead)
    test_idx <- (nrow(df_1) - steps_ahead + 1):nrow(df_1)
    
    train_series <- df_1$STOIM_log[train_idx]
    actual_future <- df_1$STOIM_log[test_idx]
    
    if (length(train_series) < 24) {
      message(sprintf("Skipping TNVED %s: series too short (%d)", tnved, length(train_series)))
      return(NULL)
    }
    
    #-----------------------------------------------------------------------
    # Fit models (with error handling)
    #-----------------------------------------------------------------------
    fc_1 <- tryCatch(sarima_fun(train_series, steps_ahead), error = function(e) {
      message(sprintf("SARIMA failed for TNVED %s: %s", tnved, e$message))
      rep(NA_real_, steps_ahead)
    })
    
    fc_2 <- tryCatch(tbats_fun(train_series, steps_ahead), error = function(e) {
      message(sprintf("TBATS failed for TNVED %s: %s", tnved, e$message))
      rep(NA_real_, steps_ahead)
    })
    
    fc_3 <- tryCatch(stl_ets_fun(train_series, steps_ahead), error = function(e) {
      message(sprintf("STL-ETS failed for TNVED %s: %s", tnved, e$message))
      rep(NA_real_, steps_ahead)
    })
    
    #-----------------------------------------------------------------------
    # Compute forecast errors
    #-----------------------------------------------------------------------
    err_1 <- evaluate_forecasts(actual_future, fc_1)
    err_2 <- evaluate_forecasts(actual_future, fc_2)
    err_3 <- evaluate_forecasts(actual_future, fc_3)
    
    tibble(
      TNVED = tnved,
      model = c("sarima", "tbats", "stl_ets"),
      RMSE = c(err_1$rmse, err_2$rmse, err_3$rmse),
      MAE = c(err_1$mae, err_2$mae, err_3$mae)
    )
  }, .progress = TRUE)
  
  #---------------------------------------------------------------------------
  # 4️⃣ Combine results
  #---------------------------------------------------------------------------
  results_df <- bind_rows(results)
  
  # Reset to sequential plan (cleanup)
  plan(sequential)
  
  message("✅ Benchmark completed.")
  return(results_df)
}
