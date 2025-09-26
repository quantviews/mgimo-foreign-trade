library(FinTS)

trade_to_pivot <- function(df, n){
  periods <- df %>% pull(PERIOD) %>% unique()
  df <- df  %>%
    mutate(first_n = substr(TNVED, start = 1, stop = n),
           last_k = substr(TNVED, start = n+1, stop = 10))
  df_new <- list()
  for (i in 1:length(periods)){
    df_per <- df %>% filter(PERIOD == periods[i])
    df_per_tnveds <- df_per %>% pull(TNVED) %>% unique()
    df_per_tnved <- list()
    for (j in 1:length(df_per_tnveds)){
      df_per_tnved[[j]] <- df_per %>%
        filter(TNVED == df_per_tnveds[j]) %>%
        arrange(TNVED) %>%
        mutate(is_group = last_k == (rep('0', 10-n) %>% str_flatten))
      if (sum(df_per_tnved[[j]]$is_group) == 0){
        df_per_tnved[[j]] <- df_per_tnved[[j]]
      }else{
        df_per_tnved[[j]] <- df_per_tnved[[j]] %>% filter(is_group == T)
      }
    }
    df_new[[i]] <- bind_rows(df_per_tnved)
  }
  bind_rows(df_new)
}

arch_pvalue <- function(series) {
  # Remove NAs just in case
  series <- na.omit(series)
  
  # Check for minimum length for ARCH test
  if (length(series) <= 12) return(NA_real_)
  
  # Try safely, and set lags to avoid exceeding length
  tryCatch({
    ts_data <- ts(series, frequency = 12)
    lags <- min(12, length(series) - 1)
    FinTS::ArchTest(ts_data, lags = lags) %>%
      pluck("p.value") %>%
      set_names(NULL)
  }, error = function(e) NA_real_)
}
