# VAR

fit_var <- function(data, max_p = 6) {
   
   ic <- ICr(data)
   r <- ic$r.star[3]
   
   factors <- ic$F_pca[, 1:r, drop = FALSE]
   
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
   
   model <- vars::VAR(
      factors,
      p = p,
      type = "const"
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

forecast_var <- function(model, h) {
   
   # Прогноз факторов
   factor_fcst <- predict(
      model$model,
      n.ahead = h
   )$fcst
   
   factors_hat <- map_dfc(
      factor_fcst,
      \(x) x[, "fcst"]
   ) %>%
      as.matrix()
   
   # Возвращаемся из пространства факторов
   # в пространство исходных переменных
   forecast <- factors_hat %*% model$loadings
   
   colnames(forecast) <- model$columns
   
   forecast
}