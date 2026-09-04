# Static

fit_static <- function(data) {
   
   list(
      columns = colnames(data)
   )
}

forecast_static <- function(model, h) {
   
   matrix(
      0,
      nrow = h,
      ncol = length(model$columns),
      dimnames = list(
         NULL,
         model$columns
      )
   )
}