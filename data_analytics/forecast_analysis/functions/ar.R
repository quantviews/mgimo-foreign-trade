# AR

fit_ar <- function(data, max_p = 12) {
   
   fit_one <- function(x) {
      
      models <- map(
         0:max_p,
         \(p) {
            tryCatch(
               stats::arima(
                  x,
                  order = c(p, 0, 0),
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
         p = best - 1,
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

forecast_ar <- function(model, h) {
   
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