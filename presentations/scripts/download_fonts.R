#!/usr/bin/env Rscript
# Однократная загрузка шрифтов для офлайн-презентации (из Google Fonts GitHub).
# Запуск из корня проекта: quarto run presentations/scripts/download_fonts.R

root_dir <- normalizePath(getwd(), winslash = "/", mustWork = TRUE)
font_dir <- file.path(root_dir, "presentations", "fonts")
dir.create(font_dir, recursive = TRUE, showWarnings = FALSE)

fonts <- c(
  "PTSans-Regular.ttf" = "https://raw.githubusercontent.com/google/fonts/main/ofl/ptsans/PT_Sans-Web-Regular.ttf",
  "PTSans-Bold.ttf" = "https://raw.githubusercontent.com/google/fonts/main/ofl/ptsans/PT_Sans-Web-Bold.ttf",
  "PTSans-Italic.ttf" = "https://raw.githubusercontent.com/google/fonts/main/ofl/ptsans/PT_Sans-Web-Italic.ttf",
  "PTSerif-Bold.ttf" = "https://raw.githubusercontent.com/google/fonts/main/ofl/ptserif/PT_Serif-Web-Bold.ttf",
  "SourceSans3-Variable.ttf" = "https://raw.githubusercontent.com/google/fonts/main/ofl/sourcesans3/SourceSans3%5Bwght%5D.ttf"
)

if (!requireNamespace("httr", quietly = TRUE)) {
  stop("Установите пакет httr: install.packages('httr')", call. = FALSE)
}

is_valid_font <- function(path) {
  if (!file.exists(path)) {
    return(FALSE)
  }
  size <- file.info(path)$size
  if (is.na(size) || size < 50000) {
    return(FALSE)
  }
  sig <- readBin(path, what = "raw", n = 4)
  # TrueType / OpenType signatures
  identical(sig, as.raw(c(0x00, 0x01, 0x00, 0x00))) ||
    identical(sig, as.raw(c(0x4F, 0x54, 0x54, 0x4F))) ||
    identical(sig, as.raw(c(0x74, 0x72, 0x75, 0x65))) ||
    identical(sig, as.raw(c(0x77, 0x4F, 0x46, 0x46)))
}

for (name in names(fonts)) {
  dest <- file.path(font_dir, name)
  if (is_valid_font(dest)) {
    message("OK: ", name)
    next
  }
  message("Загрузка: ", name)
  resp <- httr::GET(fonts[[name]], httr::write_disk(dest, overwrite = TRUE))
  if (httr::status_code(resp) != 200) {
    stop("Не удалось загрузить ", name, " (HTTP ", httr::status_code(resp), ")", call. = FALSE)
  }
}

message("Шрифты сохранены в ", font_dir)
