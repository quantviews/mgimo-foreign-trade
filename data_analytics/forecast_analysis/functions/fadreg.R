# fadreg: y_t = a + b * y_{t-1} + c * F_{t-1}

fit_fadreg <- function(
      data,
      r_max = 5,
      max_p = 3,
      max_q = 1,
      max_factor_p = 6,
      n_valid = 6
) {
   
   data <- as.matrix(data)
   
   if (anyNA(data)) {
      stop("data contains NA values.")
   }
   
   n <- nrow(data)
   k <- ncol(data)
   
   if (n < 30) {
      stop("Not enough observations.")
   }
   
   
   # ------------------------------------------------------------
   # Fit one dynamic regression
   # ------------------------------------------------------------
   
   fit_reg <- function(y, F, p, q) {
      
      dat <- tibble(y = as.numeric(y))
      
      if (p > 0) {
         for (j in seq_len(p)) {
            dat[[paste0("y_lag", j)]] <-
               dplyr::lag(y, j)
         }
      }
      
      for (j in 0:q) {
         for (f in seq_len(ncol(F))) {
            dat[[paste0("F", f, "_lag", j)]] <-
               dplyr::lag(F[, f], j)
         }
      }
      
      dat <- tidyr::drop_na(dat)
      
      if (nrow(dat) < 10) {
         return(NULL)
      }
      
      tryCatch(
         stats::lm(y ~ ., data = dat),
         error = function(e) NULL
      )
   }
   
   
   # ------------------------------------------------------------
   # Fit factor VAR and select lag by AIC
   # ------------------------------------------------------------
   
   fit_factor_var <- function(F) {
      
      p_max <- min(
         max_factor_p,
         max(1, floor(nrow(F) / 10))
      )
      
      p_selected <- tryCatch(
         vars::VARselect(
            F,
            lag.max = p_max,
            type = "const"
         )$selection[["AIC(n)"]],
         error = function(e) 1L
      )
      
      p_selected <- as.integer(p_selected)
      
      if (
         length(p_selected) != 1L ||
         !is.finite(p_selected) ||
         p_selected < 1
      ) {
         p_selected <- 1L
      }
      
      p_selected <- min(
         p_selected,
         p_max
      )
      
      model <- tryCatch(
         vars::VAR(
            F,
            p = p_selected,
            type = "const"
         ),
         error = function(e) NULL
      )
      
      if (is.null(model)) {
         return(NULL)
      }
      
      list(
         model = model,
         p = p_selected
      )
   }
   
   
   # ------------------------------------------------------------
   # One-step forecast
   # ------------------------------------------------------------
   
   forecast_one <- function(
      model,
      y,
      F,
      F_future,
      p,
      q
   ) {
      
      if (is.null(model)) {
         return(NA_real_)
      }
      
      newdata <- list()
      
      # Own lags
      if (p > 0) {
         
         for (j in seq_len(p)) {
            
            value <- y[length(y) - j + 1]
            
            if (
               length(value) != 1L ||
               !is.finite(value)
            ) {
               return(NA_real_)
            }
            
            newdata[[paste0("y_lag", j)]] <- value
         }
      }
      
      # Current and lagged factors
      for (j in 0:q) {
         
         F_values <- if (j == 0) {
            F_future
         } else {
            F[
               nrow(F) - j + 1,
               ,
               drop = FALSE
            ]
         }
         
         for (f in seq_len(ncol(F))) {
            
            value <- F_values[1, f]
            
            if (
               length(value) != 1L ||
               !is.finite(value)
            ) {
               return(NA_real_)
            }
            
            newdata[[
               paste0("F", f, "_lag", j)
            ]] <- value
         }
      }
      
      newdata <- as.data.frame(newdata)
      
      pred <- tryCatch(
         stats::predict(
            model,
            newdata = newdata
         ),
         error = function(e) NA_real_
      )
      
      if (
         length(pred) != 1L ||
         !is.finite(pred)
      ) {
         return(NA_real_)
      }
      
      as.numeric(pred)
   }
   
   
   # ============================================================
   # Rolling validation
   # ============================================================
   
   validation_points <- seq(
      n - n_valid + 1,
      n
   )
   
   validation <- vector(
      "list",
      length(validation_points)
   )
   
   
   for (v in seq_along(validation_points)) {
      
      test_t <- validation_points[v]
      train_end <- test_t - 1
      
      train <- data[
         seq_len(train_end),
         ,
         drop = FALSE
      ]
      
      actual <- data[test_t, ]
      
      message(
         "Validation ",
         v,
         "/",
         length(validation_points),
         " (t = ",
         test_t,
         ")"
      )
      
      
      # ---------------------------------------------------------
      # PCA on training sample only
      # ---------------------------------------------------------
      
      ic <- ICr(train)
      
      r_available <- min(
         r_max,
         ncol(ic$F_pca)
      )
      
      F_all <- ic$F_pca[
         ,
         seq_len(r_available),
         drop = FALSE
      ]
      
      
      # ---------------------------------------------------------
      # Factor VAR for each r
      # ---------------------------------------------------------
      
      factor_models <- vector(
         "list",
         r_available
      )
      
      for (r in seq_len(r_available)) {
         
         F <- F_all[
            ,
            seq_len(r),
            drop = FALSE
         ]
         
         factor_models[[r]] <- fit_factor_var(F)
      }
      
      
      # ---------------------------------------------------------
      # Candidate grid
      # ---------------------------------------------------------
      
      grid <- tidyr::crossing(
         r = seq_len(r_available),
         p = 0:max_p,
         q = 0:max_q
      )
      
      grid$loss <- NA_real_
      
      
      # ---------------------------------------------------------
      # Evaluate specifications
      # ---------------------------------------------------------
      
      for (g in seq_len(nrow(grid))) {
         
         r <- grid$r[g]
         p <- grid$p[g]
         q <- grid$q[g]
         
         factor_fit <- factor_models[[r]]
         
         if (is.null(factor_fit)) {
            next
         }
         
         F <- F_all[
            ,
            seq_len(r),
            drop = FALSE
         ]
         
         
         # One-step factor forecast is all we need:
         # F[t+1] is unknown; F[t], F[t-1], ... are observed.
         
         F_pred <- tryCatch(
            predict(
               factor_fit$model,
               n.ahead = 1
            )$fcst,
            error = function(e) NULL
         )
         
         if (is.null(F_pred)) {
            next
         }
         
         F_future <- matrix(
            NA_real_,
            nrow = 1,
            ncol = r
         )
         
         for (f in seq_len(r)) {
            F_future[1, f] <-
               F_pred[[f]][1, "fcst"]
         }
         
         colnames(F_future) <- colnames(F)
         
         
         # ------------------------------------------------------
         # Fit 196 regressions
         # ------------------------------------------------------
         
         forecasts <- rep(
            NA_real_,
            k
         )
         
         for (i in seq_len(k)) {
            
            model <- fit_reg(
               y = train[, i],
               F = F,
               p = p,
               q = q
            )
            
            forecasts[i] <- forecast_one(
               model = model,
               y = train[, i],
               F = F,
               F_future = F_future,
               p = p,
               q = q
            )
         }
         
         
         # ------------------------------------------------------
         # MAE
         # ------------------------------------------------------
         
         ok <- is.finite(forecasts) &
            is.finite(actual)
         
         if (any(ok)) {
            
            grid$loss[g] <- mean(
               abs(
                  forecasts[ok] -
                     actual[ok]
               )
            )
         }
      }
      
      validation[[v]] <- grid
   }
   
   
   # ============================================================
   # Aggregate validation
   # ============================================================
   
   validation <- dplyr::bind_rows(
      validation,
      .id = "validation_id"
   ) %>%
      mutate(
         validation_id = as.integer(validation_id)
      )
   
   
   selection <- validation %>%
      group_by(r, p, q) %>%
      summarise(
         MAE = mean(loss, na.rm = TRUE),
         n_valid = sum(is.finite(loss)),
         .groups = "drop"
      ) %>%
      filter(
         is.finite(MAE),
         n_valid >= ceiling(n_valid / 2)
      ) %>%
      arrange(MAE)
   
   if (nrow(selection) == 0) {
      stop("No valid FADREG specification was found.")
   }
   
   best <- selection %>%
      slice(1)
   
   
   # ============================================================
   # Final PCA
   # ============================================================
   
   ic <- ICr(data)
   
   F <- ic$F_pca[
      ,
      seq_len(best$r),
      drop = FALSE
   ]
   
   
   # ============================================================
   # Final factor VAR
   # ============================================================
   
   factor_fit <- fit_factor_var(F)
   
   
   if (is.null(factor_fit)) {
      stop("Could not fit final factor VAR.")
   }
   
   
   # ============================================================
   # Final regressions
   # ============================================================
   
   models <- map(
      seq_len(k),
      \(i) {
         fit_reg(
            y = data[, i],
            F = F,
            p = best$p,
            q = best$q
         )
      }
   )
   
   
   # ============================================================
   # Return
   # ============================================================
   
   list(
      models = models,
      
      history = data,
      
      pca = ic,
      factors = F,
      
      # Selected dynamic regression
      r = best$r,
      p = best$p,
      q = best$q,
      
      # Factor VAR
      factor_model = factor_fit$model,
      factor_p = factor_fit$p,
      
      # Diagnostics
      validation = validation,
      selection = selection,
      
      columns = colnames(data)
   )
}

forecast_fadreg <- function(model, h) {
   
   y_history <- model$history
   F_history <- model$factors
   
   p <- model$p
   q <- model$q
   r <- model$r
   
   n <- nrow(y_history)
   k <- ncol(y_history)
   
   
   # ------------------------------------------------------------
   # Forecast factors
   # ------------------------------------------------------------
   
   factor_fcst <- tryCatch(
      predict(
         model$factor_model,
         n.ahead = h + q
      )$fcst,
      error = function(e) NULL
   )
   
   if (is.null(factor_fcst)) {
      stop("Could not forecast factors.")
   }
   
   F_future <- matrix(
      NA_real_,
      nrow = h + q,
      ncol = r
   )
   
   for (f in seq_len(r)) {
      F_future[, f] <-
         factor_fcst[[f]][, "fcst"]
   }
   
   colnames(F_future) <- colnames(F_history)
   
   
   # ------------------------------------------------------------
   # Recursive forecast of y
   # ------------------------------------------------------------
   
   y_forecast <- matrix(
      NA_real_,
      nrow = h,
      ncol = k
   )
   
   colnames(y_forecast) <- model$columns
   
   
   for (step in seq_len(h)) {
      
      for (i in seq_len(k)) {
         
         reg_model <- model$models[[i]]
         
         if (is.null(reg_model)) {
            next
         }
         
         newdata <- list()
         
         
         # -----------------------------------------------------
         # Own lags
         # -----------------------------------------------------
         
         if (p > 0) {
            
            for (j in seq_len(p)) {
               
               lag_step <- step - j
               
               value <- if (lag_step <= 0) {
                  
                  y_history[
                     n + lag_step,
                     i
                  ]
                  
               } else {
                  
                  y_forecast[
                     lag_step,
                     i
                  ]
               }
               
               newdata[[
                  paste0("y_lag", j)
               ]] <- value
            }
         }
         
         
         # -----------------------------------------------------
         # Current and lagged factors
         # -----------------------------------------------------
         
         for (j in 0:q) {
            
            factor_step <- step - j
            
            F_values <- if (factor_step <= 0) {
               
               # Observed historical factor
               F_history[
                  nrow(F_history) + factor_step,
                  ,
                  drop = FALSE
               ]
               
            } else {
               
               # Forecast factor
               F_future[
                  factor_step,
                  ,
                  drop = FALSE
               ]
            }
            
            for (f in seq_len(r)) {
               
               newdata[[
                  paste0("F", f, "_lag", j)
               ]] <- F_values[1, f]
            }
         }
         
         
         # -----------------------------------------------------
         # Forecast
         # -----------------------------------------------------
         
         newdata <- as.data.frame(newdata)
         
         pred <- tryCatch(
            predict(
               reg_model,
               newdata = newdata
            ),
            error = function(e) NA_real_
         )
         
         if (
            length(pred) == 1L &&
            is.finite(pred)
         ) {
            y_forecast[
               step,
               i
            ] <- as.numeric(pred)
         }
      }
   }
   
   
   y_forecast
}