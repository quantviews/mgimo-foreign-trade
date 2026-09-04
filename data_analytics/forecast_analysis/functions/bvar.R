# BVAR

fit_bvar <- function(
      data,
      max_p = 6,
      n_draw = 10000,
      n_burn = 5000,
      n_thin = 1
) {
   
   ic <- ICr(data)
   r <- ic$r.star[3]
   
   factors <- ic$F_pca[, 1:r, drop = FALSE]
   colnames(factors) <- paste0("F", seq_len(r))
   
   # Loadings: X ≈ F %*% Lambda
   loadings <- qr.solve(
      factors,
      as.matrix(data)
   )
   
   var_sel <- vars::VARselect(
      factors,
      lag.max = max_p,
      type = "const"
   )
   
   p <- as.integer(var_sel$selection[["AIC(n)"]])
   
   # OLS residual SD of AR(p); BVAR auto_psi() uses CSS ARIMA and can fail
   psi_mode <- apply(factors, 2, \(x) {
      y <- embed(x, p + 1)
      sqrt(sum(lm.fit(cbind(1, y[, -1, drop = FALSE]), y[, 1])$residuals^2) /
              (nrow(y) - p - 1))
   })
   
   model <- BVAR::bvar(
      data = factors,
      lags = p,
      n_draw = n_draw,
      n_burn = n_burn,
      n_thin = n_thin,
      priors = BVAR::bv_priors(
         mn = BVAR::bv_minnesota(
            b = 0,
            psi = BVAR::bv_psi(mode = psi_mode)
         )
      ),
      verbose = FALSE
   )
   
   list(
      model = model,
      pca = ic,
      loadings = loadings,
      r = r,
      p = p,
      columns = colnames(data)
   )
}

forecast_bvar <- function(model, h) {
   
   # Прогноз факторов (медиана предиктивного распределения)
   factors_hat <- apply(
      predict(model$model, horizon = h)$fcast,
      c(2, 3),
      median
   )
   
   factors_hat <- matrix(
      factors_hat,
      nrow = h,
      ncol = model$r
   )
   
   # Возвращаемся из пространства факторов
   # в пространство исходных переменных
   forecast <- factors_hat %*% model$loadings
   
   colnames(forecast) <- model$columns
   
   forecast
}
