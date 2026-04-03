# Графики бюллетеня: снимок CSV или агрегат из fizob_2.parquet

strana_labels <- c(
  CN = "Китай",
  IN = "Индия",
  TR = "Турция",
  ALL = "Все страны"
)

#' Снимок уже агрегированных рядов (колонки: period, strana, napr, idx)
read_bulletin_csv <- function(path) {
  readr::read_csv(path, show_col_types = FALSE) |>
    dplyr::mutate(
      STRANA = toupper(as.character(.data$strana)),
      NAPR = as.character(.data$napr),
      PERIOD = as.Date(.data$period),
      idx = as.numeric(.data$idx)
    ) |>
    dplyr::select("STRANA", "NAPR", "PERIOD", "idx") |>
    dplyr::filter(.data$STRANA %in% names(strana_labels)) |>
    dplyr::mutate(
      series = strana_labels[.data$STRANA],
      series = forcats::fct_relevel(
        factor(.data$series),
        unname(strana_labels[c("CN", "IN", "TR", "ALL")])
      )
    )
}

#' Агрегат mean(fizob2) по TNVED2 из data_processed/fizob_2.parquet
read_fizob2_parquet <- function(path) {
  arrow::read_parquet(path) |>
    tibble::as_tibble()
}

aggregate_fizob2_bulletin <- function(df) {
  df |>
    dplyr::mutate(STRANA = toupper(as.character(STRANA))) |>
    dplyr::filter(STRANA %in% names(strana_labels)) |>
    dplyr::group_by(STRANA, NAPR, PERIOD) |>
    dplyr::summarize(
      idx = mean(.data$fizob2, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      series = strana_labels[.data$STRANA],
      series = forcats::fct_relevel(
        factor(.data$series),
        unname(strana_labels[c("CN", "IN", "TR", "ALL")])
      )
    )
}

bulletin_hc <- function(agg, napr_code, napr_title) {
  d <- agg |>
    dplyr::filter(.data$NAPR == napr_code)

  if (nrow(d) == 0) {
    return(
      highcharter::hc_chart(
        highcharter::highchart(),
        title = list(text = paste0("Нет данных: ", napr_title))
      )
    )
  }

  highcharter::hchart(
    d,
    "line",
    highcharter::hcaes(x = PERIOD, y = idx, group = series)
  ) |>
    highcharter::hc_title(
      text = paste0("Индексы физобъёмов (ТН ВЭД 2 знака), ", napr_title)
    ) |>
    highcharter::hc_subtitle(
      text = "Снимок: см. site/data/bulletin_snapshot.csv или mean(fizob2) по 2-значным группам из fizob_2.parquet"
    ) |>
    highcharter::hc_yAxis(title = list(text = "Индекс")) |>
    highcharter::hc_xAxis(type = "datetime", title = list(text = NULL)) |>
    highcharter::hc_tooltip(shared = TRUE, sort = TRUE) |>
    highcharter::hc_plotOptions(
      line = list(marker = list(enabled = FALSE), lineWidth = 2)
    ) |>
    highcharter::hc_legend(layout = "horizontal", align = "center", verticalAlign = "bottom") |>
    highcharter::hc_chart(zoomType = "x")
}
