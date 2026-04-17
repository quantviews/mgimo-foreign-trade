# Графики бюллетеня: снимок CSV или агрегат из fizob_2.parquet или fizob_total.parquet

# МГИМО цветовая палитра
mgimo_colors <- c(
  primary    = "#003d7a",    # МГИМО синий основной
  secondary  = "#e74c3c",    # Красный акцент
  accent1    = "#27ae60",    # Зелёный для позитива
  accent2    = "#3498db",    # Голубой
  accent3    = "#f39c12",    # Оранжевый
  text       = "#2c3e50",    # Тёмный серо-голубой
  light_bg   = "#f8f9fa"     # Светлый фон
)

# Цвета для линий по странам
strana_colors <- c(
  "Китай"      = "#003d7a",  # МГИМО синий
  "Индия"      = "#e74c3c",  # Красный
  "Турция"     = "#27ae60",  # Зелёный
  "Все страны" = "#f39c12"   # Оранжевый
)

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

#' Читает и агрегирует данные из fizob_total.parquet
#'
#' fizob_total.parquet содержит детальные данные по странам и товарам (TNVED2)
#' Функция агрегирует по среднему значению fizob по товарам в каждый период
#' для стран: CN, TR, IN, ALL (или создает ALL автоматически)
read_fizob_total_parquet <- function(path) {
  df <- arrow::read_parquet(path) |>
    tibble::as_tibble()

  # Выбираем только нужные страны (если ALL есть в данных)
  target_strana <- c("CN", "TR", "IN", "ALL")
  available_strana <- intersect(target_strana, unique(df$STRANA))

  # Если ALL есть, используем его. Если нет - создадим позже
  result <- df |>
    dplyr::filter(.data$STRANA %in% available_strana) |>
    dplyr::group_by(.data$STRANA, .data$NAPR, .data$PERIOD) |>
    dplyr::summarize(
      idx = mean(.data$fizob, na.rm = TRUE),
      .groups = "drop"
    )

  # Если ALL отсутствует, создаём его как средний индекс по CN, TR, IN
  if (!("ALL" %in% available_strana)) {
    other_strana <- intersect(c("CN", "TR", "IN"), unique(result$STRANA))
    all_agg <- result |>
      dplyr::filter(.data$STRANA %in% other_strana) |>
      dplyr::group_by(.data$NAPR, .data$PERIOD) |>
      dplyr::summarize(
        idx = mean(.data$idx, na.rm = TRUE),
        .groups = "drop"
      ) |>
      dplyr::mutate(STRANA = "ALL")

    result <- dplyr::bind_rows(result, all_agg)
  }

  # Форматируем выходные данные
  result |>
    dplyr::mutate(
      STRANA = toupper(as.character(.data$STRANA)),
      series = strana_labels[.data$STRANA],
      series = forcats::fct_relevel(
        factor(.data$series),
        unname(strana_labels[c("CN", "IN", "TR", "ALL")])
      )
    ) |>
    dplyr::select("STRANA", "NAPR", "PERIOD", "idx", "series")
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

  # Преобразуем series (factor) в character для маппинга цветов
  d <- d |>
    dplyr::mutate(series_name = as.character(.data$series))

  # Получаем цвета для каждой серии
  series_list <- d |>
    dplyr::select(.data$series_name) |>
    dplyr::distinct() |>
    dplyr::pull()

  colors_for_chart <- unname(strana_colors[series_list])

  highcharter::hchart(
    d,
    "line",
    highcharter::hcaes(x = PERIOD, y = idx, group = series_name)
  ) |>
    # Цвета линий
    highcharter::hc_colors(colors_for_chart) |>
    # Заголовок и подзаголовок
    highcharter::hc_title(
      text = paste0("Индексы физических объёмов (ТН ВЭД 2 знака), ", napr_title),
      style = list(
        color = mgimo_colors[["primary"]],
        fontSize = "18px",
        fontWeight = "700"
      )
    ) |>
    highcharter::hc_subtitle(
      text = "Данные из комбинированных источников (таможня, Nowcast при необходимости)",
      style = list(
        color = mgimo_colors[["text"]],
        fontSize = "13px"
      )
    ) |>
    # Оси
    highcharter::hc_yAxis(
      title = list(
        text = "Индекс",
        style = list(color = mgimo_colors[["primary"]], fontWeight = "600")
      ),
      labels = list(style = list(color = mgimo_colors[["text"]])),
      gridLineColor = "#ecf0f1",
      plotLines = list(
        list(
          value = 1.0,
          color = mgimo_colors[["secondary"]],
          width = 1.5,
          dashStyle = "shortDash",
          zIndex = 0
        )
      )
    ) |>
    highcharter::hc_xAxis(
      type = "datetime",
      title = list(text = NULL),
      labels = list(style = list(color = mgimo_colors[["text"]]))
    ) |>
    # Подсказка при наведении
    highcharter::hc_tooltip(
      shared = TRUE,
      sort = TRUE,
      backgroundColor = "#ffffff",
      borderColor = mgimo_colors[["primary"]],
      borderRadius = 4,
      style = list(
        color = mgimo_colors[["text"]],
        fontSize = "12px"
      ),
      headerFormat = "<b>{point.x:%d.%m.%Y}</b><br>",
      pointFormat = '<span style="color:{point.color}">\u25CF</span> {series.name}: <b>{point.y:.3f}</b><br>'
    ) |>
    # Легенда
    highcharter::hc_legend(
      layout = "horizontal",
      align = "center",
      verticalAlign = "bottom",
      margin = 20,
      itemStyle = list(
        color = mgimo_colors[["text"]],
        fontWeight = "500"
      ),
      backgroundColor = "#f8f9fa",
      borderColor = "#ecf0f1",
      borderRadius = 4
    ) |>
    # Расширенные опции линий
    highcharter::hc_plotOptions(
      line = list(
        marker = list(
          enabled = TRUE,
          radius = 4,
          symbol = "circle"
        ),
        lineWidth = 2.5,
        states = list(
          hover = list(
            lineWidth = 3.5,
            halo = list(size = 5)
          )
        )
      )
    ) |>
    # Зум
    highcharter::hc_chart(
      zoomType = "x",
      backgroundColor = mgimo_colors[["light_bg"]]
    ) |>
    # Экспорт
    highcharter::hc_exporting(
      enabled = TRUE,
      buttons = list(
        contextButton = list(
          menuItems = c("viewFullscreen", "downloadPNG", "downloadJPEG", "downloadPDF", "downloadSVG")
        )
      )
    )
}
