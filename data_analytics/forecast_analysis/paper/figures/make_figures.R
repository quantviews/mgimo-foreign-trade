# Figures for the nowcast-quality paper. dpi = 400.

library(tidyverse)
library(readxl)
library(scales)

root <- "D:/Работа/mgimo-foreign-trade/data_analytics/forecast_analysis"
out_dir <- file.path(root, "paper/figures")
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

raw <- read_excel(file.path(root, "results/results_table.xlsx"))
names(raw) <- str_trim(names(raw))

exp_levels <- c("2025", "IV кв. 2025", "I кв. 2026")
exp_from_col <- function(nm) {
  if (str_detect(nm, "2025$") && !str_detect(nm, "Q4|Q1")) "2025"
  else if (str_detect(nm, "Q4 2025")) "IV кв. 2025"
  else if (str_detect(nm, "Q1 2026")) "I кв. 2026"
  else NA_character_
}

long <- raw %>%
  pivot_longer(
    -c(model, Time_sec),
    names_to = "column",
    values_to = "value"
  ) %>%
  mutate(
    metric = case_when(
      str_starts(column, "MAE_") ~ "MAE",
      str_starts(column, "RMSE_") ~ "RMSE",
      str_starts(column, "WMAE_") ~ "WMAE",
      TRUE ~ NA_character_
    ),
    experiment = vapply(column, exp_from_col, character(1)),
    experiment = factor(experiment, levels = exp_levels)
  )

# Ranking: MAE and WMAE in each window + time (as in the bake-off bulletin)
quality_ranks <- long %>%
  filter(metric %in% c("MAE", "WMAE")) %>%
  group_by(metric, experiment) %>%
  mutate(place = rank(value, ties.method = "min")) %>%
  ungroup()

speed_ranks <- raw %>%
  transmute(
    model,
    metric = "Время",
    experiment = NA,
    value = Time_sec,
    place = rank(Time_sec, ties.method = "min")
  )

ranking <- bind_rows(
  quality_ranks %>% select(model, metric, experiment, place),
  speed_ranks %>% select(model, metric, experiment, place)
) %>%
  group_by(model) %>%
  summarise(mean_place = mean(place), .groups = "drop") %>%
  mutate(overall = rank(mean_place, ties.method = "min")) %>%
  arrange(overall, mean_place)

model_order <- ranking$model

write_csv(ranking, file.path(out_dir, "ranking.csv"))

theme_paper <- function(base_size = 11) {
  theme_bw(base_size = base_size) +
    theme(
      text = element_text(color = "black"),
      panel.grid.minor = element_blank(),
      panel.grid.major.x = element_blank(),
      legend.position = "bottom",
      legend.title = element_blank(),
      legend.background = element_blank(),
      strip.background = element_rect(fill = "grey95", colour = "grey70"),
      axis.title.x = element_text(margin = margin(t = 6)),
      axis.title.y = element_text(margin = margin(r = 6)),
      plot.margin = margin(6, 10, 6, 6)
    )
}

exp_cols <- c(
  "2025" = "#1a1a1a",
  "IV кв. 2025" = "#b03a2e",
  "I кв. 2026" = "#1f618d"
)

save_fig <- function(p, name, width = 7.2, height = 5.4) {
  ggsave(
    file.path(out_dir, paste0(name, ".png")),
    p,
    width = width,
    height = height,
    dpi = 400,
    bg = "white"
  )
  ggsave(
    file.path(out_dir, paste0(name, ".pdf")),
    p,
    width = width,
    height = height,
    device = cairo_pdf,
    bg = "white"
  )
}

# --- Fig. 1: WMAE Cleveland ---
wmae <- long %>%
  filter(metric == "WMAE") %>%
  mutate(model = factor(model, levels = rev(model_order)))

wmae_rng <- wmae %>%
  group_by(model) %>%
  summarise(xmin = min(value), xmax = max(value), .groups = "drop")

p_wmae <- ggplot() +
  geom_linerange(
    data = wmae_rng,
    aes(xmin = xmin, xmax = xmax, y = model),
    color = "grey65",
    linewidth = 0.7
  ) +
  geom_point(
    data = wmae,
    aes(x = value, y = model, color = experiment, shape = experiment),
    size = 2.4
  ) +
  scale_color_manual(values = exp_cols) +
  scale_shape_manual(values = c(16, 17, 15)) +
  labs(x = "WMAE (взвешенная MAE)", y = NULL) +
  theme_paper()

save_fig(p_wmae, "fig_wmae", width = 7.2, height = 5.6)

# --- Fig. 2: MAE Cleveland ---
mae <- long %>%
  filter(metric == "MAE") %>%
  mutate(model = factor(model, levels = rev(model_order)))

mae_rng <- mae %>%
  group_by(model) %>%
  summarise(xmin = min(value), xmax = max(value), .groups = "drop")

p_mae <- ggplot() +
  geom_linerange(
    data = mae_rng,
    aes(xmin = xmin, xmax = xmax, y = model),
    color = "grey65",
    linewidth = 0.7
  ) +
  geom_point(
    data = mae,
    aes(x = value, y = model, color = experiment, shape = experiment),
    size = 2.4
  ) +
  scale_color_manual(values = exp_cols) +
  scale_shape_manual(values = c(16, 17, 15)) +
  labs(x = "MAE", y = NULL) +
  theme_paper()

save_fig(p_mae, "fig_mae", width = 7.2, height = 5.6)

# --- Fig. 3: accuracy vs time ---
acc_time <- raw %>%
  mutate(
    wmae_mean = (`WMAE_2025` + `WMAE_Q4 2025` + `WMAE_Q1 2026`) / 3,
    model = factor(model, levels = model_order),
    winner = model == "Avg(SARIMA, ETS+DFM)"
  )

p_time <- ggplot(acc_time, aes(x = Time_sec, y = wmae_mean)) +
  geom_point(aes(color = winner), size = 2.6) +
  ggrepel::geom_text_repel(
    aes(label = model),
    size = 2.7,
    family = "sans",
    max.overlaps = 20,
    box.padding = 0.28,
    point.padding = 0.15,
    segment.color = "grey55",
    min.segment.length = 0
  ) +
  scale_color_manual(values = c("FALSE" = "grey25", "TRUE" = "#b03a2e"), guide = "none") +
  scale_x_continuous(trans = "sqrt", breaks = c(0, 1, 5, 20, 50, 100, 150)) +
  labs(
    x = "Медианное время оценки и прогноза, сек. (окно IV кв. 2025)",
    y = "Средняя WMAE по трём окнам"
  ) +
  theme_paper()

save_fig(p_time, "fig_time", width = 7.2, height = 5.2)

# --- Fig. 4: rank heatmap ---
rank_heat <- quality_ranks %>%
  mutate(
    score = paste(metric, as.character(experiment), sep = ", "),
    model = factor(model, levels = rev(model_order))
  )

score_levels <- c(
  paste("MAE", exp_levels, sep = ", "),
  paste("WMAE", exp_levels, sep = ", ")
)
rank_heat$score <- factor(rank_heat$score, levels = score_levels)

p_heat <- ggplot(rank_heat, aes(x = score, y = model, fill = place)) +
  geom_tile(color = "white", linewidth = 0.4) +
  geom_text(aes(label = place), size = 2.8, color = "grey10") +
  scale_fill_gradient(
    low = "#d5f5e3",
    high = "#f5b7b1",
    name = "Место"
  ) +
  labs(x = NULL, y = NULL) +
  theme_paper() +
  theme(
    axis.text.x = element_text(angle = 35, hjust = 1, vjust = 1, size = 8),
    panel.grid = element_blank(),
    legend.position = "right"
  )

save_fig(p_heat, "fig_ranks", width = 7.4, height = 5.6)

# --- Fig. 5: WMAE relative to static ---
static_wmae <- long %>%
  filter(metric == "WMAE", model == "Static") %>%
  select(experiment, static = value)

rel <- long %>%
  filter(metric == "WMAE", model != "Static") %>%
  left_join(static_wmae, by = "experiment") %>%
  mutate(
    rel = value / static,
    model = factor(model, levels = rev(model_order))
  )

p_rel <- ggplot(rel, aes(x = rel, y = model, color = experiment, shape = experiment)) +
  geom_vline(xintercept = 1, linetype = "dashed", color = "grey40") +
  geom_point(size = 2.4, position = position_dodge(width = 0.45)) +
  scale_color_manual(values = exp_cols) +
  scale_shape_manual(values = c(16, 17, 15)) +
  labs(
    x = "WMAE относительно static (меньше 1 — лучше константы)",
    y = NULL
  ) +
  theme_paper()

save_fig(p_rel, "fig_relative", width = 7.2, height = 5.6)

# LaTeX table of headline metrics
tab <- raw %>%
  left_join(ranking, by = "model") %>%
  arrange(overall) %>%
  transmute(
    model,
    overall,
    MAE_2025 = sprintf("%.3f", MAE_2025),
    WMAE_2025 = sprintf("%.3f", WMAE_2025),
    MAE_Q4 = sprintf("%.3f", `MAE_Q4 2025`),
    WMAE_Q4 = sprintf("%.3f", `WMAE_Q4 2025`),
    MAE_Q1 = sprintf("%.3f", `MAE_Q1 2026`),
    WMAE_Q1 = sprintf("%.3f", `WMAE_Q1 2026`),
    Time = sprintf("%.1f", Time_sec)
  )

write_csv(tab, file.path(out_dir, "results_for_tex.csv"))

cat("Wrote figures to ", out_dir, "\n", sep = "")
print(ranking)
