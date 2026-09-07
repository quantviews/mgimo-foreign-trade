# Seasonal naive

fit_snaive <- function(data, period = 12) {
   
   n <- nrow(data)
   p <- min(period, n)
   
   list(
      seasonal = as.matrix(data[(n - p + 1):n, , drop = FALSE]),
      period = p,
      columns = colnames(data)
   )
}

forecast_snaive <- function(model, h) {
   
   idx <- ((seq_len(h) - 1) %% model$period) + 1
   
   forecast <- model$seasonal[idx, , drop = FALSE]
   colnames(forecast) <- model$columns
   
   forecast
}
