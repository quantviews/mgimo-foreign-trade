#!/usr/bin/env Rscript
# Код графиков — из site/bulletin_comparison.qmd.
# Запуск из корня проекта mgimo-foreign-trade:
#   quarto run presentations/scripts/export_bulletin_charts.R

root_dir <- normalizePath(getwd(), winslash = "/", mustWork = TRUE)
site_dir <- file.path(root_dir, "site")
pres_dir <- file.path(root_dir, "presentations")
out_dir <- file.path(pres_dir, "figures", "bulletin")

if (!file.exists(file.path(pres_dir, "project-overview.qmd"))) {
  stop(
    "Запустите из корня проекта mgimo-foreign-trade (там, где лежат site/ и presentations/).",
    call. = FALSE
  )
}
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

owd <- getwd()
on.exit(setwd(owd), add = TRUE)
setwd(site_dir)
library(tidyverse)
library(arrow)
library(plotly)
library(htmltools)
library(htmlwidgets)

# ── шрифт: меняйте только здесь ──────────────────────────────────────────
viz_font_family <- "Source Sans 3"
viz_font_size <- 13
# ───────────────────────────────────────────────────────────────────────────

mgimo_colors <- c(
  primary    = "#003d7a",
  secondary  = "#e74c3c",
  accent1    = "#27ae60",
  accent2    = "#3498db",
  accent3    = "#f39c12",
  text       = "#2c3e50",
  light_bg   = "#f8f9fa"
)

bulletin_fo <- arrow::read_parquet("data/bulletin_fo.parquet")
tab_stoim_oil <- arrow::read_parquet("data/tab_stoim_oil.parquet")

pl_fizob <-
  bulletin_fo %>%
  mutate(STRANA = case_when(
    STRANA == "CN" ~ "Китай",
    STRANA == "TR" ~ "Турция",
    STRANA == "IN" ~ "Индия",
    STRANA == "OTHER" ~ "Прочие"
  ) %>% factor(
    levels = c(
      "Китай",
      "Индия",
      "Турция",
      "Прочие"
    )
  )) %>%
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
  facet_wrap(~STRANA, scales = "free",
    labeller = as_labeller(\(x) paste0("<b>", x, "</b>"))
  ) +
  scale_y_continuous(
    labels = scales::percent_format(accuracy = 1, scale = 1, style_positive = "plus"),
    expand = c(0, 0)
  ) +
  scale_x_date(
    date_breaks = "1 years", date_labels = "%Y",
    expand = c(0.01, 0.01)
  ) +
  scale_color_manual(
    values = c(mgimo_colors[["primary"]], mgimo_colors[["secondary"]]),
    breaks = c("ИМ", "ЭК")
  ) +
  labs(
    x = NULL,
    y = NULL,
    color = NULL
  ) +
  theme_classic(base_size = viz_font_size, base_family = viz_font_family) +
  coord_cartesian(clip = "off") +
  theme(
    legend.position = "bottom",
    legend.key.width = unit(1.1, "cm"),
    strip.background = element_blank(),
    axis.text.x = element_text(size = 10),
    panel.spacing.y = unit(0.5, "cm"),
    panel.spacing.x = unit(0.1, "cm"),
    panel.background = element_rect(fill = "transparent"),
    plot.background = element_rect(fill = "transparent"),
    legend.background = element_rect(fill = "transparent"),
  )

pl_oil_nonoil <-
  tab_stoim_oil %>%
  mutate(STRANA = case_when(
    STRANA == "CN" ~ "Китай",
    STRANA == "TR" ~ "Турция",
    STRANA == "IN" ~ "Индия",
    STRANA == "OTHER" ~ "Прочие"
  ) %>% factor(
    levels = c(
      "Китай",
      "Индия",
      "Турция",
      "Прочие"
    )
  ),
  STOIM = round(STOIM, 3)
  ) %>%
  rename(
    Дата = PERIOD,
    Тип = type,
    `Экспорт, млрд долл.` = STOIM
  ) %>%
  ggplot(aes(
    x = Дата,
    y = `Экспорт, млрд долл.`,
    color = Тип
  )) +
  geom_hline(
    yintercept = 0,
    linetype = "dashed",
    color = "grey50",
    linewidth = 0.3
  ) +
  geom_line() +
  facet_wrap(~STRANA, scales = "free",
    labeller = as_labeller(\(x) paste0("<b>", x, "</b>"))
  ) +
  scale_y_continuous(expand = c(0, 0)) +
  scale_x_date(
    date_breaks = "1 years", date_labels = "%Y",
    expand = c(0.01, 0.01)
  ) +
  scale_color_manual(
    values = c(mgimo_colors[["accent1"]], mgimo_colors[["accent3"]]),
    breaks = c("Кроме нефти и газа", "Нефтегазовый")
  ) +
  labs(
    x = NULL,
    y = NULL,
    color = NULL
  ) +
  theme_classic(base_size = viz_font_size, base_family = viz_font_family) +
  coord_cartesian(clip = "off") +
  theme(
    legend.position = "bottom",
    legend.key.width = unit(2.5, "cm"),
    strip.background = element_blank(),
    axis.text.x = element_text(size = 10),
    panel.spacing.y = unit(0.5, "cm"),
    panel.spacing.x = unit(0.1, "cm"),
    panel.background = element_rect(fill = "transparent", color = "transparent"),
    plot.background = element_rect(fill = "transparent", color = "transparent"),
    legend.background = element_rect(fill = "transparent", color = "transparent")
  )

# ggplotly не переносит ширину легенды из ggplot: узкий clip-path и сдвиг 2-го пункта.
save_bulletin_plotly <- function(p, file, fix_legend = FALSE) {
  w <- ggplotly(p, dynamicTicks = FALSE) %>%
    layout(
      font = list(family = viz_font_family, size = viz_font_size),
      legend = list(orientation = "h", x = 0.5, xanchor = "center", y = -0.06),
      paper_bgcolor = "rgba(0,0,0,0)",
      plot_bgcolor = "rgba(0,0,0,0)"
    )

  saveRDS(w, sub("\\.html$", ".rds", file))

  saveWidget(w, file = file, selfcontained = TRUE)

  legend_css <- if (fix_legend) {
    ".legend .scrollbox{clip-path:none!important}.legend .groups:nth-child(2){transform:translateX(183px)!important}"
  } else {
    ""
  }

  font_style <- paste0(
    "<style>",
    "@font-face{font-family:'Source Sans 3';font-style:normal;font-weight:400 700;",
    "src:url('../../fonts/SourceSans3-Variable.ttf') format('truetype');}",
    "html,body{background:transparent!important;}",
    legend_css,
    "</style>"
  )

  html <- readLines(file, warn = FALSE, encoding = "UTF-8")
  html <- gsub(
    "<link[^>]*fonts\\.googleapis\\.com[^>]*>\\s*",
    "",
    html,
    perl = TRUE
  )
  if (!any(grepl("SourceSans3-Variable", html, fixed = TRUE))) {
    html <- sub("</head>", paste0(font_style, "\n</head>"), html, fixed = TRUE)
  }
  html <- gsub("background-color:\\s*white", "background-color:transparent", html, ignore.case = TRUE)
  writeLines(html, file, useBytes = TRUE)

  files_dir <- sub("\\.html$", "_files", file)
  if (dir.exists(files_dir)) {
    unlink(files_dir, recursive = TRUE)
  }
}

save_bulletin_plotly(pl_fizob, file.path(out_dir, "fizob.html"))
save_bulletin_plotly(pl_oil_nonoil, file.path(out_dir, "oil_nonoil.html"), fix_legend = TRUE)
