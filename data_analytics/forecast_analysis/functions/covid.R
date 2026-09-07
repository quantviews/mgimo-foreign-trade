# AR with COVID dummy (train starts at `start`)

fit_covid <- function(
      data,
      max_p = 12,
      start = as.Date("2019-02-01"),
      dummy_from = as.Date("2020-03-01"),
      dummy_to = as.Date("2020-12-01")
) {
   
   dates <- seq(start, by = "month", length.out = nrow(data))
   
   dummy <- matrix(
      as.numeric(dates >= dummy_from & dates <= dummy_to),
      ncol = 1,
      dimnames = list(NULL, "covid")
   )
   
   fit_one <- function(x) {
      
      models <- map(
         0:max_p,
         \(p) {
            
            fit <- tryCatch(
               stats::arima(
                  x,
                  order = c(p, 0, 0),
                  include.mean = TRUE,
                  method = "ML",
                  xreg = dummy
               ),
               error = function(e) NULL
            )
            
            if (!is.null(fit)) {
               fit$call$xreg <- dummy
            }
            
            fit
         }
      )
      
      aic <- map_dbl(
         models,
         \(model) {
            if (is.null(model)) Inf else AIC(model)
         }
      )
      
      best <- which.min(aic)
      
      if (!is.finite(aic[[best]])) {
         
         fit <- stats::arima(
            x,
            order = c(0, 0, 0),
            include.mean = TRUE,
            method = "CSS",
            xreg = dummy
         )
         fit$call$xreg <- dummy
         
         list(model = fit, p = 0)
         
      } else {
         
         list(
            model = models[[best]],
            p = best - 1
         )
      }
   }
   
   list(
      models = map(data, fit_one),
      dummy = dummy,
      columns = colnames(data)
   )
}

forecast_covid <- function(model, h) {
   
   dummy <- model$dummy
   newxreg <- matrix(0, nrow = h, ncol = 1, dimnames = list(NULL, "covid"))
   
   map_dfc(
      model$models,
      \(x) {
         predict(x$model, n.ahead = h, newxreg = newxreg)$pred
      }
   ) %>%
      set_names(model$columns) %>%
      as.matrix()
}
