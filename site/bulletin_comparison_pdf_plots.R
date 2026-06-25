# Стили и объекты графиков для печатной PDF-версии (альбомная A4).

country_colors <- c(
  "Китай" = "#D73027",
  "Индия" = "#F4A261",
  "Япония" = "#CC79A7",
  "Турция" = "#7B3294",
  "Венгрия" = "#1B9E77",
  "Словакия" = "#377EB8",
  "Прочие" = "grey75"
)

theme_pdf_base <- function() {
  theme(
    text = element_text(color = mgimo_colors[["text"]]),
    plot.background = element_rect(fill = "white", color = NA),
    panel.background = element_rect(fill = "white", color = NA),
    plot.margin = margin(6, 8, 4, 4)
  )
}

theme_pdf_timeseries <- function() {
  theme_classic(base_size = 10) +
    theme_pdf_base() +
    theme(
      legend.position = "bottom",
      legend.key.width = unit(0.85, "cm"),
      legend.text = element_text(size = 8.5),
      legend.margin = margin(t = 2),
      strip.background = element_rect(fill = mgimo_colors[["light_bg"]], color = NA),
      strip.text = element_text(face = "bold", size = 9, color = mgimo_colors[["primary"]]),
      axis.text = element_text(size = 8, color = mgimo_colors[["text"]]),
      axis.line = element_line(linewidth = 0.35, color = "grey40"),
      panel.spacing.y = unit(0.55, "cm"),
      panel.spacing.x = unit(0.55, "cm")
    )
}

theme_pdf_area <- function() {
  theme_classic(base_size = 10) +
    theme_pdf_base() +
    theme(
      legend.position = "bottom",
      legend.key.width = unit(0.55, "cm"),
      legend.text = element_text(size = 7.5),
      legend.margin = margin(t = 2),
      strip.background = element_rect(fill = mgimo_colors[["light_bg"]], color = NA),
      strip.text = element_text(face = "bold", size = 9, color = mgimo_colors[["primary"]]),
      axis.text = element_text(size = 8, color = mgimo_colors[["text"]]),
      axis.line = element_line(linewidth = 0.35, color = "grey40"),
      panel.spacing.y = unit(0.35, "cm"),
      panel.spacing.x = unit(0.45, "cm")
    )
}

theme_pdf_groups <- function() {
  theme_minimal(base_size = 10) +
    theme_pdf_base() +
    theme(
      panel.grid.minor = element_blank(),
      panel.grid.major.y = element_blank(),
      panel.grid.major.x = element_line(linewidth = 0.22, color = "grey90"),
      legend.position = "none",
      axis.text.y = element_text(size = 7.8, color = mgimo_colors[["text"]]),
      axis.text.x = element_text(size = 8, color = mgimo_colors[["text"]]),
      axis.title.x = element_text(size = 8.5, color = "grey40")
    )
}

data_oilgas <- arrow::read_parquet("data/data_oilgas.parquet")

pl_oilgas <-
  data_oilgas %>%
  ggplot(aes(x = PERIOD, y = NETTO, fill = STRANA)) +
  geom_area(position = "stack", linewidth = 0.1, color = "white") +
  facet_wrap(~good, scales = "free", labeller = facet_labeller) +
  scale_x_date(expand = expansion(mult = 0)) +
  scale_y_continuous(expand = expansion(mult = c(0, 0.02))) +
  scale_fill_manual(
    values = country_colors,
    breaks = c("Китай", "Индия", "Турция", "Япония", "Венгрия", "Словакия", "Прочие")
  ) +
  labs(x = NULL, y = NULL, fill = NULL) +
  theme_pdf_area() +
  coord_cartesian(clip = "off")

pl_fizob_pdf <- pl_fizob + theme_pdf_timeseries()
pl_oilgas_pdf <- pl_oilgas + theme_pdf_area()
plot_export_pdf <- plot_export + theme_pdf_groups() + labs(x = "млрд долл.")
plot_import_pdf <- plot_import + theme_pdf_groups() + labs(x = "млрд долл.")
