fit_ma <- function(data, max_q = 12) {
   
   fit_one <- function(x) {
      
      models <- map(
         0:max_q,
         \(q) {
            tryCatch(
               stats::arima(
                  x,
                  order = c(0, 0, q),
                  include.mean = TRUE,
                  method = "ML"
               ),
               error = function(e) NULL
            )
         }
      )
      
      aic <- map_dbl(
         models,
         \(model) {
            if (is.null(model)) Inf else AIC(model)
         }
      )
      
      best <- which.min(aic)
      
      list(
         model = models[[best]],
         q = best - 1,
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

forecast_ma <- function(model, h) {
   
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