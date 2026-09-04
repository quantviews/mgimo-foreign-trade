# DFM

fit_dfm <- function(data, max_p = 6, max_p_final = 2) {
   
   # Выбор числа факторов
   ic <- ICr(data)
   r <- ic$r.star[3]
   
   # Выбор числа лагов VAR для факторов
   var_sel <- vars::VARselect(
      ic$F_pca[, 1:r, drop = FALSE],
      lag.max = max_p,
      type = "const"
   )
   
   p_selected <- as.integer(var_sel$selection[["AIC(n)"]])
   
   # Оставляем твоё ограничение p <= 2
   p <- min(p_selected, max_p_final)
   
   # Оценка DFM
   model <- DFM(
      data,
      r = r,
      p = p
   )
   
   list(
      model = model,
      pca = ic,
      r = r,
      p = p,
      p_selected = p_selected,
      columns = colnames(data)
   )
}

forecast_dfm <- function(model, h) {
   
   forecast <- predict(
      model$model,
      h = h,
      standardized = FALSE
   )$X_fcst %>%
      as.matrix()
   
   colnames(forecast) <- model$columns
   
   forecast
}