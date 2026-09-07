# STL + DFM: снимаем сезонность, DFM на SA, в прогнозе сезон возвращаем

fit_dfm_stl <- function(
      data,
      max_p = 6,
      max_p_final = 2,
      frequency = 12
) {
   
   data <- as.matrix(data)
   n <- nrow(data)
   k <- ncol(data)
   
   seasonal <- matrix(
      0,
      nrow = frequency,
      ncol = k,
      dimnames = list(NULL, colnames(data))
   )
   
   sa <- data
   
   if (n >= 2 * frequency) {
      
      for (j in seq_len(k)) {
         
         decomp <- stl_sa_one(data[, j], frequency)
         sa[, j] <- decomp$sa
         seasonal[, j] <- decomp$cycle
      }
   }
   
   list(
      dfm = fit_dfm(sa, max_p = max_p, max_p_final = max_p_final),
      seasonal = seasonal,
      frequency = frequency,
      columns = colnames(data)
   )
}

forecast_dfm_stl <- function(model, h) {
   
   sa_fc <- forecast_dfm(model$dfm, h)
   
   idx <- ((seq_len(h) - 1) %% model$frequency) + 1
   
   forecast <- sa_fc + model$seasonal[idx, , drop = FALSE]
   colnames(forecast) <- model$columns
   
   forecast
}

stl_sa_one <- function(x, frequency) {
   
   x <- as.numeric(x)
   
   if (length(x) < 2 * frequency) {
      return(list(sa = x, cycle = rep(0, frequency)))
   }
   
   stl_fit <- tryCatch(
      stats::stl(
         ts(x, frequency = frequency),
         s.window = "periodic",
         robust = TRUE
      ),
      error = function(e) NULL
   )
   
   if (is.null(stl_fit)) {
      return(list(sa = x, cycle = rep(0, frequency)))
   }
   
   seas <- as.numeric(stl_fit$time.series[, "seasonal"])
   list(sa = x - seas, cycle = tail(seas, frequency))
}
