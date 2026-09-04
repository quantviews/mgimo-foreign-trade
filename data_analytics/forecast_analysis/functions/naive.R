# Naive

fit_naive <- function(data) {
   
   list(
      last = data[nrow(data), ],
      columns = colnames(data)
   )
}

forecast_naive <- function(model, h) {
   
   matrix(
      rep(
         as.numeric(model$last),
         each = h
      ),
      nrow = h,
      ncol = length(model$columns),
      dimnames = list(
         NULL,
         model$columns
      )
   )
}