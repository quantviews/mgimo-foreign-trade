# ETS + DFM: ETS снимает сезон (если выбирает его), DFM на SA

fit_dfm_ets <- function(
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
   
   for (j in seq_len(k)) {
      
      decomp <- ets_sa_one(data[, j], frequency)
      sa[, j] <- decomp$sa
      seasonal[, j] <- decomp$cycle
   }
   
   list(
      dfm = fit_dfm(sa, max_p = max_p, max_p_final = max_p_final),
      seasonal = seasonal,
      frequency = frequency,
      columns = colnames(data)
   )
}

forecast_dfm_ets <- function(model, h) {
   
   sa_fc <- forecast_dfm(model$dfm, h)
   idx <- ((seq_len(h) - 1) %% model$frequency) + 1
   forecast <- sa_fc + model$seasonal[idx, , drop = FALSE]
   colnames(forecast) <- model$columns
   forecast
}

ets_sa_one <- function(x, frequency) {
   
   x <- as.numeric(x)
   n <- length(x)
   zero <- list(sa = x, cycle = rep(0, frequency))
   
   if (n < 2 * frequency) {
      return(zero)
   }
   
   fit <- tryCatch(
      forecast::ets(
         ts(x, frequency = frequency),
         additive.only = TRUE
      ),
      error = function(e) NULL
   )
   
   if (is.null(fit) || isTRUE(fit$components[3] == "N")) {
      return(zero)
   }
   
   st <- as.matrix(fit$states)
   fitv <- tryCatch(as.numeric(stats::fitted(fit)), error = function(e) NULL)
   
   if (is.null(fitv) || length(fitv) != n || nrow(st) < n) {
      return(stl_sa_one(x, frequency))
   }
   
   st_t <- if (nrow(st) == n + 1) st[seq_len(n), , drop = FALSE] else st[seq_len(n), , drop = FALSE]
   l <- st_t[, 1]
   b <- if (isTRUE(fit$components[2] != "N") && ncol(st_t) >= 2) st_t[, 2] else 0
   seas <- fitv - l - b
   
   if (!all(is.finite(seas))) {
      return(stl_sa_one(x, frequency))
   }
   
   list(sa = x - seas, cycle = tail(seas, frequency))
}
