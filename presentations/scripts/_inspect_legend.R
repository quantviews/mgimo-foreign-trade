library(jsonlite)
h <- readLines("presentations/figures/bulletin/oil_nonoil.html", warn = FALSE)
j <- grep('data-for="htmlwidget', h, value = TRUE)[1]
j <- sub(".*>", "", sub("</script>.*", "", j))
x <- fromJSON(j)
cat("legend:\n")
print(x$x$layout$legend)
cat("\nmargin:\n")
print(x$x$layout$margin)
cat("\nlegend traces:\n")
for (i in seq_along(x$x$data)) {
  if (isTRUE(x$x$data[[i]]$showlegend)) {
    cat(i, ":", x$x$data[[i]]$name, "\n")
  }
}
