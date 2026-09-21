# Интерактивные/веб-графики для HTML-версий страновых бюллетеней.
# Данные (trade_balance, data_oil, df_groups) уже загружены в самом бюллетене;
# здесь только построение в стиле сводного бюллетеня (bulletin_comparison):
# plotly для временных рядов + SVG-«градусник» по категориям с веб-размерами.
# Подключается ТОЛЬКО в html-ветке (eval: knitr::is_html_output()).

suppressWarnings(suppressMessages({
  library(plotly)
  library(patchwork)
}))

# общая веб-тема временных рядов (как в сводном бюллетене)
.web_ts_theme <- function() {
  theme_classic(base_size = 13, base_family = "Source Sans Pro") +
    theme(
      legend.position = "bottom",
      legend.key.width = unit(1.1, "cm"),
      strip.background = element_blank(),
      axis.text.x = element_text(size = 10),
      panel.background = element_rect(fill = "transparent"),
      plot.background = element_rect(fill = "transparent"),
      legend.background = element_rect(fill = "transparent")
    )
}

# Экспорт/импорт/сальдо: линии + столбец сальдо
web_balance <- function(trade_balance, colors) {
  d <- trade_balance |>
    dplyr::mutate(
      `Стоимость, млрд долл.` = round(STOIM / 1e9, 1),
      Показатель = dplyr::case_when(
        NAPR == "ЭК" ~ "Экспорт",
        NAPR == "ИМ" ~ "Импорт",
        NAPR == "ТБ" ~ "Сальдо"
      )
    ) |>
    dplyr::rename(Дата = PERIOD)
  dmin <- min(d$Дата); dmax <- max(d$Дата)
  ggplot(d) +
    geom_col(data = ~ dplyr::filter(.x, Показатель == "Сальдо"),
             aes(Дата, `Стоимость, млрд долл.`, fill = Показатель)) +
    geom_line(data = ~ dplyr::filter(.x, Показатель != "Сальдо"),
              aes(Дата, `Стоимость, млрд долл.`, color = Показатель), linewidth = I(1)) +
    scale_color_manual(values = c("Экспорт" = colors[["secondary"]], "Импорт" = colors[["primary"]]), name = NULL) +
    scale_fill_manual(values = c("Сальдо" = colors[["accent1"]]), name = NULL) +
    scale_x_date(breaks = seq(dmin, dmax, by = "year"), labels = scales::label_date("%Y")) +
    labs(x = NULL, y = NULL) +
    .web_ts_theme() +
    coord_cartesian(clip = "off")
}

# Экспорт нефти/нефтепродуктов в натуральном выражении
web_oil <- function(data_oil, colors) {
  d <- data_oil |>
    dplyr::filter(NAPR == "ЭК") |>
    dplyr::mutate(`Экспорт, млн т` = round(NETTO, 1)) |>
    dplyr::rename(Дата = PERIOD)
  dmin <- min(d$Дата); dmax <- max(d$Дата)
  ggplot(d, aes(Дата, `Экспорт, млн т`)) +
    geom_line(color = colors[["primary"]], linewidth = I(1)) +
    geom_point(shape = 21, fill = "white", color = colors[["primary"]]) +
    scale_x_date(breaks = seq(dmin, dmax, by = "year"), labels = scales::label_date("%Y")) +
    labs(x = NULL, y = NULL) +
    .web_ts_theme() +
    theme(legend.position = "none")
}

# один временной ряд -> интерактивный plotly (легенда снизу, шрифт сайта)
render_ts <- function(p) {
  ggplotly(p, dynamicTicks = FALSE) |>
    plotly::layout(
      font = list(family = "Source Sans Pro", size = 13),
      legend = list(orientation = "h", x = 0.5, xanchor = "center", y = -0.12,
                    font = list(family = "Source Sans Pro", size = 13))
    )
}

# тема «градусника» по категориям (веб-размеры), заголовок в рамке
.web_groups_theme <- function(site_bg) {
  theme_minimal(base_size = 16, base_family = "Source Sans Pro") +
    theme(
      panel.grid.minor = element_blank(),
      panel.grid.major.y = element_blank(),
      legend.position = "none",
      panel.background = element_rect(fill = site_bg, colour = NA),
      plot.background = element_rect(fill = site_bg, colour = NA),
      plot.title.position = "plot",
      plot.title = ggtext::element_textbox_simple(
        size = 16, face = "bold", family = "Source Sans Pro", color = "#003d7a",
        halign = 0.02, padding = margin(3, 8, 3, 8), margin = margin(0, 0, 6, 0),
        fill = "transparent", box.color = "#003d7a", linewidth = 1.5, r = unit(5, "pt")
      ),
      axis.title.x = element_text(size = 13, color = "grey40")
    )
}

web_group_plot <- function(df_groups, napr, title, exclude, colors, site_bg) {
  df_groups |>
    dplyr::filter(NAPR == napr, !TNVED4 %in% exclude) |>
    dplyr::mutate(
      dplyr::across(c(STOIM_last12, STOIM_year_before, STOIM_diff), ~ .x * 1000),
      type = dplyr::if_else(STOIM_diff > 0, "positive", "negative"),
      position_label = (STOIM_last12 + STOIM_year_before) / 2,
      label = dplyr::if_else(STOIM_diff > 0, paste0("+", round(STOIM_diff, 1)), paste0(round(STOIM_diff, 1)))
    ) |>
    dplyr::arrange(-abs(STOIM_diff)) |>
    head(14) |>
    dplyr::mutate(TNVED4_string = forcats::fct_reorder(TNVED4_string, STOIM_last12)) |>
    ggplot(aes(color = type)) +
    geom_segment(aes(x = STOIM_year_before, xend = STOIM_last12, y = TNVED4_string, yend = TNVED4_string), linewidth = 0.7) +
    geom_point(aes(x = STOIM_last12, y = TNVED4_string), fill = site_bg, shape = 21, size = 2.5) +
    scale_color_manual(values = c(colors[["accent1"]], colors[["secondary"]]), breaks = c("positive", "negative")) +
    geom_text(aes(x = position_label, y = TNVED4_string, label = label), nudge_y = 0.42, size = 4) +
    coord_cartesian(clip = "off") +
    .web_groups_theme(site_bg) +
    labs(y = NULL, x = "млн долл.", title = title)
}

# экспорт + импорт «градусники» -> один SVG, шрифт приводим к сайту
render_groups_svg <- function(df_groups, exclude, colors, site_bg, path) {
  g <- web_group_plot(df_groups, "ЭК", "Экспорт", exclude, colors, site_bg) /
       web_group_plot(df_groups, "ИМ", "Импорт", exclude, colors, site_bg)
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  ggsave(path, plot = g, width = 10, height = 11, device = svglite::svglite, bg = site_bg)
  svg <- readLines(path, warn = FALSE, encoding = "UTF-8")
  svg <- gsub('font-family: "[^"]+"', 'font-family: "Source Sans Pro"', svg, perl = TRUE)
  writeLines(svg, path, useBytes = TRUE)
  path
}
