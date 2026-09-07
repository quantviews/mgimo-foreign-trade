# ETS / Holt-Winters (additive; series already differenced)

fit_ets <- function(data, frequency = 12) {
   
   fit_one <- function(x) {
      
      x_ts <- ts(as.numeric(x), frequency = frequency)
      
      m <- tryCatch(
         forecast::ets(x_ts, additive.only = TRUE),
         error = function(e) NULL
      )
      
      if (is.null(m)) {
         m <- forecast::ets(x_ts, model = "ANN")
      }
      
      m
   }
   
   models <- map(data, fit_one)
   
   list(
      models = models,
      columns = colnames(data)
   )
}

forecast_ets <- function(model, h) {
   
   map_dfc(
      model$models,
      \(x) {
         as.numeric(forecast::forecast(x, h = h)$mean)
      }
   ) %>%
      set_names(model$columns) %>%
      as.matrix()
}
