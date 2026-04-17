#!/usr/bin/env Rscript

library(arrow, quietly = TRUE)
library(dplyr, quietly = TRUE)
library(tibble, quietly = TRUE)

# Читаем parquet файл
df <- read_parquet('data_processed/fizob_total.parquet')

# Основная информация
cat("=== СТРУКТУРА ФАЙЛА fizob_total.parquet ===\n")
cat("Размер: ", nrow(df), " строк, ", ncol(df), " колонок\n\n")

# Названия колонок
cat("Колонки:\n")
for (col in names(df)) {
  cat("  -", col, "\n")
}
cat("\n")

# Первые строки
cat("=== ПЕРВЫЕ 20 СТРОК ===\n")
print(tibble::as_tibble(df) %>% head(20))
cat("\n")

# Уникальные значения
cat("=== УНИКАЛЬНЫЕ ЗНАЧЕНИЯ ===\n")
cat("STRANA:", paste(unique(df$STRANA), collapse = ", "), "\n")
cat("NAPR:", paste(unique(df$NAPR), collapse = ", "), "\n")
if ("TNVED" %in% names(df)) {
  cat("TNVED (примеры):", paste(head(unique(df$TNVED), 10), collapse = ", "), "...\n")
}
cat("PERIOD (min-max):", as.character(min(df$PERIOD, na.rm = TRUE)), "-", as.character(max(df$PERIOD, na.rm = TRUE)), "\n")
cat("\n")

# Статистика по fizob
cat("FIZOB статистика:\n")
print(summary(df$fizob))
cat("\n")

# Количество записей по странам и направлениям
cat("=== КОЛИЧЕСТВО ЗАПИСЕЙ ===\n")
counts <- df %>%
  group_by(STRANA, NAPR) %>%
  summarize(n = n(), .groups = "drop") %>%
  arrange(STRANA, NAPR)
print(counts)
