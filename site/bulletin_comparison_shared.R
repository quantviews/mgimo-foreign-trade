# Общая подготовка данных и графиков для bulletin_comparison (HTML и PDF).
# Перед source() задайте bulletin_output: "html" (по умолчанию) или "pdf".

if (!exists("bulletin_output", inherits = FALSE)) {
  bulletin_output <- "html"
}

for_pdf <- identical(bulletin_output, "pdf")

suppressPackageStartupMessages({
  library(tidyverse)
  library(arrow)
  library(stringi)
  if (!for_pdf) {
    library(plotly)
    library(ggbreak)
  } else if (requireNamespace("ggbreak", quietly = TRUE)) {
    library(ggbreak)
  }
})

mgimo_colors <- c(
  primary   = "#003d7a",
  secondary = "#e74c3c",
  accent1   = "#27ae60",
  accent2   = "#3498db",
  accent3   = "#f39c12",
  text      = "#2c3e50",
  light_bg  = "#f8f9fa"
)

site_bg <- if (for_pdf) "#ffffff" else mgimo_colors[["light_bg"]]

facet_labeller <- if (for_pdf) {
  label_value
} else {
  as_labeller(\(x) paste0("<b>", x, "</b>"))
}

theme_groups <- function() {
  theme_minimal(base_size = if (for_pdf) 10 else 16) +
    theme(
      panel.grid.minor = element_blank(),
      panel.grid.major.y = element_blank(),
      panel.grid.major.x = element_line(linewidth = if (for_pdf) 0.25 else 0.4),
      legend.position = "none",
      axis.text.y = element_text(size = if (for_pdf) 8.5 else rel(1)),
      axis.text.x = element_text(size = if (for_pdf) 8.5 else rel(1)),
      axis.title.x = element_text(size = if (for_pdf) 9 else rel(1)),
      panel.background = element_rect(fill = site_bg, colour = NA),
      plot.background = element_rect(fill = site_bg, colour = NA),
      panel.border = element_rect(colour = NA, fill = NA),
      plot.margin = margin(if (for_pdf) 4 else 5, if (for_pdf) 6 else 5, if (for_pdf) 2 else 5, if (for_pdf) 2 else 5)
    )
}

theme_timeseries <- function() {
  theme_classic(base_size = if (for_pdf) 10 else 13) +
    theme(
      legend.position = "bottom",
      legend.key.width = unit(if (for_pdf) 0.9 else 1.1, "cm"),
      legend.text = element_text(size = if (for_pdf) 9 else rel(1)),
      strip.background = element_rect(
        fill = if (for_pdf) mgimo_colors[["light_bg"]] else NA,
        color = NA
      ),
      strip.text = element_text(
        face = "bold",
        size = if (for_pdf) 9.5 else rel(1),
        color = if (for_pdf) mgimo_colors[["text"]] else "black"
      ),
      axis.text = element_text(size = if (for_pdf) 8.5 else rel(1)),
      axis.line = element_line(linewidth = if (for_pdf) 0.35 else 0.4),
      panel.spacing.y = unit(if (for_pdf) 0.65 else 3, "cm"),
      panel.spacing.x = unit(if (for_pdf) 0.65 else 3, "cm"),
      panel.background = element_rect(
        fill = if (for_pdf) "white" else "transparent",
        color = if (for_pdf) "white" else "transparent"
      ),
      plot.background = element_rect(
        fill = if (for_pdf) "white" else "transparent",
        color = if (for_pdf) "white" else "transparent"
      ),
      legend.background = element_rect(
        fill = if (for_pdf) "white" else "transparent",
        color = if (for_pdf) "white" else "transparent"
      ),
      plot.margin = margin(if (for_pdf) 4 else 5, if (for_pdf) 4 else 5, if (for_pdf) 2 else 5, if (for_pdf) 4 else 5)
    )
}

patch_ggbreak_svg <- function(path, bg = site_bg) {
  bg <- toupper(bg)
  svg <- readLines(path, warn = FALSE, encoding = "UTF-8")
  svg <- gsub("#FFFFFF", bg, svg, fixed = TRUE)
  svg <- gsub("#EBEBEB", bg, svg, fixed = TRUE)
  writeLines(svg, path, useBytes = TRUE)
}

country_levels <- c("Китай", "Индия", "Турция", "Прочие")

rename_countries <- function(df) {
  df %>%
    mutate(
      STRANA = case_when(
        STRANA == "CN" ~ "Китай",
        STRANA == "TR" ~ "Турция",
        STRANA == "IN" ~ "Индия",
        STRANA == "OTHER" ~ "Прочие"
      ) %>%
        factor(levels = country_levels)
    )
}

bulletin_fo <- arrow::read_parquet("data/bulletin_fo.parquet")
tab_stoim_oil <- arrow::read_parquet("data/tab_stoim_oil.parquet")
df_groups <- arrow::read_parquet("data/df_groups.parquet")
names_4 <-
  read_parquet("data/hs4_labels.parquet") %>%
  select(
    TNVED4 = hs4,
    TNVED4_string = name_ru_short
  )

pl_fizob <-
  bulletin_fo %>%
  rename_countries() %>%
  mutate(fizob = 100 * round(fizob, 3)) %>%
  rename(
    Дата = PERIOD,
    Направление = NAPR,
    `Изменение физобъёма, %` = fizob
  ) %>%
  ggplot(aes(
    x = Дата,
    y = `Изменение физобъёма, %`,
    color = Направление
  )) +
  geom_hline(
    yintercept = 0,
    linetype = "dashed",
    color = "grey50",
    linewidth = 0.3
  ) +
  geom_line() +
  facet_wrap(~STRANA, scales = "free", labeller = facet_labeller) +
  scale_y_continuous(
    labels = scales::percent_format(accuracy = 1, scale = 1, style_positive = "plus"),
    expand = c(0, 0)
  ) +
  scale_x_date(
    date_breaks = "1 years",
    date_labels = "%Y",
    expand = c(0.01, 0.01)
  ) +
  scale_color_manual(
    values = c(mgimo_colors[["primary"]], mgimo_colors[["secondary"]]),
    breaks = c("ИМ", "ЭК")
  ) +
  labs(x = NULL, y = NULL, color = NULL) +
  theme_timeseries() +
  coord_cartesian(clip = "off")

pl_oil_nonoil <-
  tab_stoim_oil %>%
  rename_countries() %>%
  mutate(STOIM = round(STOIM, 3)) %>%
  rename(
    Дата = PERIOD,
    Тип = type,
    `Экспорт, млрд. долл.` = STOIM
  ) %>%
  ggplot(aes(
    x = Дата,
    y = `Экспорт, млрд. долл.`,
    color = Тип
  )) +
  geom_hline(
    yintercept = 0,
    linetype = "dashed",
    color = "grey50",
    linewidth = 0.3
  ) +
  geom_line() +
  facet_wrap(~STRANA, scales = "free", labeller = facet_labeller) +
  scale_y_continuous(expand = c(0, 0)) +
  scale_x_date(
    date_breaks = "1 years",
    date_labels = "%Y",
    expand = c(0.01, 0.01)
  ) +
  scale_color_manual(
    values = c(mgimo_colors[["accent1"]], mgimo_colors[["accent3"]]),
    breaks = c("Кроме нефти и газа", "Нефтегазовый")
  ) +
  labs(x = NULL, y = NULL, color = NULL) +
  theme_timeseries() +
  coord_cartesian(clip = "off")

build_groups_plot <- function(napr) {
  top_n <- if (for_pdf) 18 else 20
  label_size <- if (for_pdf) 3 else 3.5

  p <- df_groups %>%
    left_join(names_4, by = "TNVED4") %>%
    filter(NAPR == napr) %>%
    mutate(
      STOIM_diff_abs = abs(STOIM_diff),
      type = if_else(STOIM_diff > 0, "positive", "negative"),
      position_label = (STOIM_last12 + STOIM_year_before) / 2,
      label = if_else(
        STOIM_diff > 0,
        paste0("+", round(STOIM_diff, 1)),
        paste0(round(STOIM_diff, 1))
      )
    ) %>%
    arrange(-STOIM_diff_abs) %>%
    head(top_n) %>%
    mutate(TNVED4_string = fct_reorder(TNVED4_string, STOIM_last12)) %>%
    ggplot(aes(color = type)) +
    geom_segment(
      aes(
        x = STOIM_year_before,
        xend = STOIM_last12,
        y = TNVED4_string,
        yend = TNVED4_string
      ),
      linewidth = if (for_pdf) 0.45 else 0.5
    ) +
    geom_point(aes(x = STOIM_last12, y = TNVED4_string), fill = site_bg, shape = 21, size = if (for_pdf) 1.8 else 2) +
    scale_color_manual(
      values = c(mgimo_colors[["accent1"]], mgimo_colors[["secondary"]]),
      breaks = c("positive", "negative")
    ) +
    geom_text(
      aes(x = position_label, y = TNVED4_string, label = label),
      nudge_y = if (for_pdf) 0.35 else 0.42,
      size = label_size
    ) +
    coord_cartesian(clip = "off") +
    theme_groups() +
    labs(y = NULL, x = "Млрд. долл.")

  if (identical(napr, "ЭК") && requireNamespace("ggbreak", quietly = TRUE)) {
    p <- p +
      ggbreak::scale_x_break(c(38, 98)) +
      geom_vline(
        xintercept = c(39.5, 97.5),
        color = "grey50",
        linetype = "solid",
        linewidth = 1.7
      )
  }

  p
}

plot_export <- build_groups_plot("ЭК")
plot_import <- build_groups_plot("ИМ")
