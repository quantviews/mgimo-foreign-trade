# SARIMA on already-differenced series: (p,0,q)(P,0,Q)[period]

fit_sarima <- function(
      data,
      max_p = 2,
      max_q = 2,
      max_P = 1,
      max_Q = 1,
      period = 12
) {
   
   fit_one <- function(x) {
      
      candidates <- tidyr::crossing(
         p = 0:max_p,
         q = 0:max_q,
         P = 0:max_P,
         Q = 0:max_Q
      ) %>%
         pmap(
            function(p, q, P, Q) {
               tryCatch(
                  stats::arima(
                     x,
                     order = c(p, 0, q),
                     seasonal = list(order = c(P, 0, Q), period = period),
                     include.mean = TRUE,
                     method = "ML"
                  ),
                  error = function(e) {
                     tryCatch(
                        stats::arima(
                           x,
                           order = c(p, 0, q),
                           seasonal = list(order = c(P, 0, Q), period = period),
                           include.mean = TRUE,
                           method = "CSS"
                        ),
                        error = function(e2) NULL
                     )
                  }
               )
            }
         )
      
      aic <- map_dbl(
         candidates,
         \(model) {
            a <- if (is.null(model)) NA_real_ else unname(model$aic)
            if (length(a) != 1L || !is.finite(a)) Inf else a
         }
      )
      
      best <- which.min(aic)
      
      if (!is.finite(aic[[best]])) {
         return(list(
            model = stats::arima(
               x,
               order = c(0, 0, 0),
               include.mean = TRUE,
               method = "CSS"
            )
         ))
      }
      
      list(model = candidates[[best]])
   }
   
   list(
      models = map(data, fit_one),
      columns = colnames(data)
   )
}

forecast_sarima <- function(model, h) {
   
   map_dfc(
      model$models,
      \(x) {
         predict(x$model, n.ahead = h)$pred
      }
   ) %>%
      set_names(model$columns) %>%
      as.matrix()
}
