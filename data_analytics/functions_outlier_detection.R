library(tidyverse)
library(forecast)

#----------------------------------------------------
# Функции для прогнозов с доверительными интервалами:
#----------------------------------------------------

sarima_fun <- function(y, h) {
  ts_y <- y %>% ts(frequency = 12)
  fit <- auto.arima(ts_y, seasonal = TRUE, stepwise = TRUE, approximation = TRUE)
  fc <- forecast(fit, h = h, level = 99)
  
  tibble(
    mean = as.numeric(fc$mean),
    lower = as.numeric(fc$lower[, "99%"]),
    upper = as.numeric(fc$upper[, "99%"])
  )
}

tbats_fun <- function(y, h) {
  ts_y <- y %>% ts(frequency = 12)
  fit <- tbats(ts_y)
  fc <- forecast(fit, h = h, level = 99)
  
  tibble(
    mean = as.numeric(fc$mean),
    lower = as.numeric(fc$lower[, "99%"]),
    upper = as.numeric(fc$upper[, "99%"])
  )
}

#--------------------------------
# Функции для выделения выбросов:
#--------------------------------

show_outliers <- function(x, nsd, tv){
  z <- (x - mean(x, na.rm = TRUE)) / sd(x, na.rm = TRUE)
  sum((abs(z) > nsd) & (x > tv), na.rm = T)
}

outlier_frac <- function(x, y, nsd, tv){
  z <- x / y
  z_sc <- (z - mean(z, na.rm = TRUE)) / sd(z, na.rm = TRUE)
  sum(
    (abs(z_sc > nsd)) & (x > tv),
    na.rm = T)
}