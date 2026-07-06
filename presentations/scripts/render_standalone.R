#!/usr/bin/env Rscript
# Standalone-презентация для передачи заказчику (один HTML-файл).
# Запуск из корня проекта mgimo-foreign-trade:
#   quarto run presentations/scripts/render_standalone.R

root_dir <- normalizePath(getwd(), winslash = "/", mustWork = TRUE)
if (!file.exists(file.path(root_dir, "presentations", "project-overview.qmd"))) {
  stop("Запустите из корня проекта mgimo-foreign-trade.", call. = FALSE)
}

steps <- c(
  "presentations/scripts/download_fonts.R",
  "presentations/scripts/export_bulletin_charts.R"
)
for (script in steps) {
  message("\n=== ", script, " ===")
  status <- system2("quarto", c("run", script), stdout = "", stderr = "")
  if (!identical(status, 0L)) {
    stop("Ошибка: ", script, call. = FALSE)
  }
}

message("\n=== quarto render (profile dist) ===")
status <- system2(
  "quarto",
  c(
    "render", "presentations/project-overview.qmd",
    "--profile", "dist"
  ),
  stdout = "",
  stderr = ""
)
if (!identical(status, 0L)) {
  stop("Ошибка рендера standalone-презентации.", call. = FALSE)
}

out <- file.path(root_dir, "presentations", "project-overview-standalone.html")
if (!file.exists(out)) {
  stop("Файл не создан: ", out, call. = FALSE)
}

size_mb <- round(file.info(out)$size / 1024^2, 1)
message("\nГотово: ", out, " (", size_mb, " MB)")
