# ARIMA

fit_arima <- function(data, max_p = 6, max_q = 6) {
   
   fit_one <- function(x) {
      
      candidates <- crossing(
         p = 0:max_p,
         q = 0:max_q
      ) %>%
         pmap(
            function(p, q) {
               tryCatch(
                  stats::arima(
                     x,
                     order = c(p, 0, q),
                     include.mean = TRUE,
                     method = "ML"
                  ),
                  error = function(e) NULL
               )
            }
         )
      
      aic <- map_dbl(
         candidates,
         \(model) {
            if (is.null(model)) Inf else AIC(model)
         }
      )
      
      best <- which.min(aic)
      
      list(
         model = candidates[[best]],
         aic = aic[[best]]
      )
   }
   
   models <- map(
      data,
      fit_one
   )
   
   list(
      models = models,
      columns = colnames(data)
   )
}

forecast_arima <- function(model, h) {
   
   map_dfc(
      model$models,
      \(x) {
         predict(
            x$model,
            n.ahead = h
         )$pred
      }
   ) %>%
      set_names(model$columns) %>%
      as.matrix()
}