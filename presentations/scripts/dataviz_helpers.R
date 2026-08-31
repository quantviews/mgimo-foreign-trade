# Графики для презентации "Визуализация данных и бизнес-аналитика".
#
# Все пары "плохо/хорошо" строятся на ОДНИХ И ТЕХ ЖЕ данных и с одинаковыми
# пределами осей: отличается только оформление. Это принципиально — иначе
# сравнение нечестное.
#
# Источники: site/data/*.parquet (в git) и data_processed/fizob_2.parquet
# (пересобирается пайплайном, см. docs/documentation_fizob.md).

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(tidyr)
  library(ggplot2)
  library(scales)
})

# --- палитра проекта (совпадает с site/bulletin_plots.R) -----------------
viz <- list(
  navy   = "#003d7a",
  red    = "#e74c3c",
  green  = "#27ae60",
  blue   = "#3498db",
  orange = "#f39c12",
  ink    = "#2c3e50",
  grey   = "#aab4c0",
  faint  = "#dde3ea",
  paper  = "#f8f9fb"
)

# --- шрифты --------------------------------------------------------------
viz_register_fonts <- function(font_dir = "fonts") {
  if (!requireNamespace("systemfonts", quietly = TRUE)) return("sans")
  reg <- file.path(font_dir, "PTSans-Regular.ttf")
  if (!file.exists(reg)) return("sans")
  ok <- try(systemfonts::register_font(
    name   = "PT Sans Viz",
    plain  = reg,
    bold   = file.path(font_dir, "PTSans-Bold.ttf"),
    italic = file.path(font_dir, "PTSans-Italic.ttf")
  ), silent = TRUE)
  if (inherits(ok, "try-error")) "sans" else "PT Sans Viz"
}

VIZ_FONT <- "sans"

# --- базовая тема --------------------------------------------------------
theme_viz <- function(base_size = 15) {
  theme_minimal(base_size = base_size, base_family = VIZ_FONT) +
    theme(
      plot.title       = element_text(face = "bold", colour = viz$navy,
                                      size = base_size * 1.25, hjust = 0,
                                      margin = margin(b = 4)),
      plot.subtitle    = element_text(colour = "#5e6b7a", size = base_size * 0.9,
                                      hjust = 0, margin = margin(b = 12)),
      plot.caption     = element_text(colour = "#8a95a3", size = base_size * 0.72,
                                      hjust = 0, margin = margin(t = 10)),
      plot.title.position = "plot",
      plot.caption.position = "plot",
      axis.title       = element_text(colour = "#5e6b7a", size = base_size * 0.82),
      axis.text        = element_text(colour = viz$ink, size = base_size * 0.82),
      panel.grid.minor = element_blank(),
      panel.grid.major = element_line(colour = viz$faint, linewidth = 0.4),
      legend.position  = "top",
      legend.justification = "left",
      legend.title     = element_blank(),
      legend.text      = element_text(size = base_size * 0.85),
      legend.key.width = unit(22, "pt"),
      plot.background  = element_rect(fill = "transparent", colour = NA),
      panel.background = element_rect(fill = "transparent", colour = NA),
      plot.margin      = margin(6, 14, 4, 6)
    )
}

# «Плохая» тема: всё по умолчанию, серая заливка, ничего не подписано.
theme_viz_bad <- function(base_size = 15) {
  theme_grey(base_size = base_size, base_family = VIZ_FONT) +
    theme(
      plot.title = element_text(size = base_size * 1.1),
      plot.background = element_rect(fill = "transparent", colour = NA),
      plot.margin = margin(6, 10, 4, 6)
    )
}

# --- загрузка данных -----------------------------------------------------
# root — корень репозитория относительно текущей рабочей директории.
viz_data <- function(root = "..") {
  p <- function(...) file.path(root, ...)
  out <- list(
    headline = as.data.frame(read_parquet(p("site/data/bulletin_headline.parquet"))),
    oil      = as.data.frame(read_parquet(p("site/data/tab_stoim_oil.parquet"))),
    fo       = as.data.frame(read_parquet(p("site/data/bulletin_fo.parquet"))),
    groups   = as.data.frame(read_parquet(p("site/data/df_groups.parquet"))),
    labels   = as.data.frame(read_parquet(p("site/data/hs4_labels.parquet")))
  )
  f2 <- p("data_processed/fizob_2.parquet")
  out$fizob2 <- if (file.exists(f2)) as.data.frame(read_parquet(f2)) else NULL
  out
}

# Результат SQL-запроса со слайда «запрос — график». Кэшируется скриптом
# scripts/dataviz_cache.R, чтобы график строился ровно по тем строкам,
# которые вернул показанный на слайде запрос.
EXAMPLE_CACHE <- "figures/dataviz/example_cn_export.parquet"

viz_example_data <- function(cache = EXAMPLE_CACHE) {
  if (!file.exists(cache)) stop("Нет кэша ", cache, " — запустите scripts/dataviz_cache.R")
  as.data.frame(read_parquet(cache))
}

strana_ru <- c(CN = "Китай", IN = "Индия", TR = "Турция",
               OTHER = "Прочие страны", ALL = "Все страны")

# =========================================================================
# КЕЙС 1. Спагетти: 98 товарных групп на одном графике
# =========================================================================
SPAGHETTI_CACHE <- "figures/dataviz/spaghetti_cn_import.parquet"

# Срез для «спагетти»: 98 групп ТН ВЭД-2, импорт из Китая.
# Источник — data_processed/fizob_2.parquet (13 МБ, вне git). Чтобы презентация
# собиралась без полного пайплайна, срез закэширован в figures/dataviz/.
# Пересобрать кэш: Rscript scripts/dataviz_cache.R
viz_spaghetti_data <- function(d, cache = SPAGHETTI_CACHE) {
  if (!is.null(d$fizob2)) {
    return(d$fizob2 |>
      filter(STRANA == "CN", NAPR == "ИМ", PERIOD >= as.Date("2022-01-01")) |>
      select(TNVED2, PERIOD, fizob2))
  }
  if (file.exists(cache)) return(as.data.frame(read_parquet(cache)))
  stop("Нет ни data_processed/fizob_2.parquet, ни кэша ", cache)
}

p_spaghetti_bad <- function(sp) {
  ggplot(sp, aes(PERIOD, fizob2, colour = TNVED2)) +
    geom_line(linewidth = 0.5) +
    scale_colour_hue() +
    scale_x_date(date_labels = "%Y") +
    coord_cartesian(ylim = c(0, 4)) +
    labs(title = "fizob2 by TNVED2", x = "PERIOD", y = "fizob2") +
    guides(colour = guide_legend(ncol = 6, keyheight = unit(6, "pt"))) +
    theme_viz_bad() +
    theme(legend.position = "right",
          legend.text = element_text(size = 4),
          legend.title = element_text(size = 5),
          legend.key.size = unit(5, "pt"))
}

p_spaghetti_good <- function(sp, focus = c("84", "87", "85")) {
  focus_lab <- c("84" = "84 — оборудование и техника",
                 "87" = "87 — транспортные средства",
                 "85" = "85 — электроника")
  sp <- sp |> mutate(is_focus = TNVED2 %in% focus)
  last_pt <- sp |> filter(is_focus) |> group_by(TNVED2) |>
    filter(PERIOD == max(PERIOD)) |> ungroup()

  ggplot() +
    geom_line(data = filter(sp, !is_focus),
              aes(PERIOD, fizob2, group = TNVED2),
              colour = viz$grey, alpha = 0.28, linewidth = 0.35) +
    geom_hline(yintercept = 1, colour = viz$ink, linewidth = 0.4, linetype = "22") +
    geom_line(data = filter(sp, is_focus),
              aes(PERIOD, fizob2, colour = TNVED2), linewidth = 1.25) +
    geom_point(data = last_pt, aes(PERIOD, fizob2, colour = TNVED2), size = 2.2) +
    scale_colour_manual(values = c("84" = viz$navy, "87" = viz$red, "85" = viz$green),
                        labels = focus_lab) +
    scale_x_date(date_labels = "%Y", date_breaks = "1 year") +
    coord_cartesian(ylim = c(0, 4)) +
    labs(
      title = "Оборудования из Китая везут вдвое больше, транспорта — вдвое меньше",
      subtitle = "Индекс физического объёма импорта, 2022 = 1 · остальные 95 групп ТН ВЭД — серым",
      x = NULL, y = "индекс объёма",
      caption = "Источник: национальная статистика КНР, расчёты проекта «Национальная торговая статистика»"
    ) +
    theme_viz()
}

# =========================================================================
# КЕЙС 2. Доллары против тонн: одна торговля, два вывода
# =========================================================================
p_usd_vs_volume <- function(d, napr = "ЭК") {
  h <- d$headline |>
    filter(NAPR == napr, PERIOD >= as.Date("2022-01-01"), nowcast_share == 0) |>
    select(PERIOD, stoim_yoy, netto_yoy) |>
    pivot_longer(-PERIOD, names_to = "metric", values_to = "yoy") |>
    filter(!is.na(yoy)) |>
    mutate(metric = recode(metric,
                           stoim_yoy = "Стоимость, $ (г/г)",
                           netto_yoy = "Физический объём, т (г/г)"))
  lastp <- h |> group_by(metric) |> filter(PERIOD == max(PERIOD)) |> ungroup()

  ggplot(h, aes(PERIOD, yoy, colour = metric)) +
    geom_hline(yintercept = 0, colour = viz$ink, linewidth = 0.5) +
    geom_line(linewidth = 1.2) +
    geom_point(data = lastp, size = 2.4) +
    scale_colour_manual(values = c("Стоимость, $ (г/г)" = viz$red,
                                   "Физический объём, т (г/г)" = viz$navy)) +
    scale_y_continuous(labels = percent_format(accuracy = 1)) +
    scale_x_date(date_labels = "%Y", date_breaks = "1 year") +
    labs(
      title = "Стоимость и объём рассказывают разные истории",
      subtitle = "Изменение экспорта год к году: в долларах и в тоннах",
      x = NULL, y = NULL,
      caption = "Разрыв между линиями — это цена. Один и тот же экспорт, два разных вывода."
    ) +
    theme_viz()
}

# =========================================================================
# КЕЙС 3. Пирог против сортированного столбика
# =========================================================================
viz_structure_data <- function(d, napr = "ЭК", top_n = 10) {
  lab <- d$labels
  key <- intersect(c("TNVED4", "hs4", "code"), names(lab))[1]
  nm  <- intersect(c("label_ru_short", "label_ru", "label", "name_ru"), names(lab))[1]
  lab <- lab |> transmute(TNVED4 = as.character(.data[[key]]),
                          label  = as.character(.data[[nm]]))

  d$groups |>
    filter(NAPR == napr) |>
    slice_max(STOIM_last12, n = top_n) |>
    left_join(lab, by = "TNVED4") |>
    mutate(
      label = ifelse(is.na(label), TNVED4, label),
      label = sub("^\\s*\\d+\\s*[—-]\\s*", "", label),
      label = paste0(TNVED4, " · ", label),
      label = ifelse(nchar(label) > 44, paste0(substr(label, 1, 42), "…"), label),
      share = STOIM_last12 / sum(STOIM_last12)
    ) |>
    arrange(desc(STOIM_last12))
}

p_pie_bad <- function(st) {
  ggplot(st, aes(x = "", y = STOIM_last12, fill = label)) +
    geom_col(width = 1, colour = "white", linewidth = 0.3) +
    coord_polar(theta = "y") +
    scale_fill_hue() +
    labs(title = "Структура экспорта, топ-10 позиций ТН ВЭД") +
    guides(fill = guide_legend(ncol = 1, keyheight = unit(9, "pt"))) +
    theme_void(base_family = VIZ_FONT) +
    theme(legend.text = element_text(size = 7),
          legend.title = element_blank(),
          plot.title = element_text(size = 13, hjust = 0.5),
          plot.background = element_rect(fill = "transparent", colour = NA))
}

ENERGY_HS4 <- c("2709", "2710", "2711", "2701", "2799")

p_bar_good <- function(st) {
  energy_share <- sum(st$share[st$TNVED4 %in% ENERGY_HS4])
  st <- st |> mutate(label = factor(label, levels = rev(label)),
                     energy = TNVED4 %in% ENERGY_HS4)
  ggplot(st, aes(STOIM_last12, label, fill = energy)) +
    geom_col(width = 0.72) +
    geom_text(aes(label = percent(share, accuracy = 0.1)),
              hjust = -0.15, size = 3.6, colour = viz$ink, family = VIZ_FONT) +
    scale_fill_manual(values = c(`TRUE` = viz$navy, `FALSE` = viz$grey), guide = "none") +
    scale_x_continuous(expand = expansion(mult = c(0, 0.14)),
                       labels = label_number()) +
    labs(
      title = paste0("Пять энергетических позиций — ",
                     percent(energy_share, accuracy = 1), " всего топ-10"),
      subtitle = "Топ-10 позиций ТН ВЭД-4 по стоимости экспорта, последние 12 месяцев",
      x = "млрд $", y = NULL,
      caption = "Отсортировано по величине, доли подписаны, цветом выделено то, о чём идёт речь"
    ) +
    theme_viz() +
    theme(panel.grid.major.y = element_blank())
}

# =========================================================================
# КЕЙС 4. Агрегат прячет историю
# =========================================================================
p_aggregate_bad <- function(d) {
  tot <- d$oil |> filter(NAPR == "ЭК") |>
    summarise(STOIM = sum(STOIM), .by = PERIOD)
  ggplot(tot, aes(PERIOD, STOIM)) +
    geom_line(linewidth = 1.2, colour = viz$navy) +
    scale_x_date(date_labels = "%Y", date_breaks = "1 year") +
    scale_y_continuous(labels = label_number(), limits = c(0, NA)) +
    labs(title = "Экспорт России", subtitle = "Всего, млрд $ в месяц",
         x = NULL, y = "млрд $") +
    theme_viz()
}

p_decomposed_good <- function(d) {
  dec <- d$oil |> filter(NAPR == "ЭК") |>
    summarise(STOIM = sum(STOIM), .by = c(PERIOD, STRANA)) |>
    mutate(series = factor(strana_ru[STRANA],
                           levels = c("Прочие страны", "Китай", "Индия", "Турция")))
  lastp <- dec |> group_by(series) |> filter(PERIOD == max(PERIOD)) |> ungroup()

  ggplot(dec, aes(PERIOD, STOIM, colour = series)) +
    geom_line(linewidth = 1.2) +
    geom_point(data = lastp, size = 2.4) +
    scale_colour_manual(values = c("Прочие страны" = viz$grey, "Китай" = viz$navy,
                                   "Индия" = viz$red, "Турция" = viz$green)) +
    scale_x_date(date_labels = "%Y", date_breaks = "1 year") +
    scale_y_continuous(labels = label_number(), limits = c(0, NA)) +
    labs(
      title = "Экспорт не столько упал, сколько сменил адрес",
      subtitle = "Тот же ряд, разложенный по странам-получателям, млрд $ в месяц",
      x = NULL, y = "млрд $",
      caption = "Итог почти не изменился: 34 → 31 млрд $. Состав изменился полностью."
    ) +
    theme_viz()
}

# =========================================================================
# КЕЙС 5. Ось: одна серия — два впечатления
# =========================================================================
viz_axis_data <- function(d) {
  d$headline |>
    filter(NAPR == "ЭК", nowcast_share == 0, PERIOD >= as.Date("2024-07-01")) |>
    select(PERIOD, stoim_bn)
}

p_axis_bad <- function(ax) {
  ggplot(ax, aes(PERIOD, stoim_bn)) +
    geom_col(fill = viz$red, width = 22) +
    coord_cartesian(ylim = c(min(ax$stoim_bn) - 0.4, max(ax$stoim_bn) + 0.4)) +
    scale_x_date(date_labels = "%m.%y", date_breaks = "3 months") +
    labs(title = "Обвал экспорта!", x = NULL, y = "млрд $") +
    theme_viz(base_size = 14) +
    theme(plot.title = element_text(colour = viz$red))
}

p_axis_good <- function(ax) {
  ggplot(ax, aes(PERIOD, stoim_bn)) +
    geom_col(fill = viz$navy, width = 22) +
    scale_y_continuous(limits = c(0, NA), expand = expansion(mult = c(0, 0.08)),
                       labels = label_number(suffix = " млрд $")) +
    scale_x_date(date_labels = "%m.%y", date_breaks = "3 months") +
    labs(title = "Экспорт колеблется в коридоре 22–32 млрд $", x = NULL, y = NULL) +
    theme_viz(base_size = 14)
}

# =========================================================================
# КЕЙС 6. Заголовок: описание против вывода
# =========================================================================
viz_india_data <- function(d) {
  d$fo |> filter(NAPR == "ЭК", STRANA %in% c("CN", "IN", "TR")) |>
    mutate(series = strana_ru[STRANA])
}

p_title_before <- function(fo) {
  ggplot(fo, aes(PERIOD, fizob, colour = series)) +
    geom_line(linewidth = 0.9) +
    scale_y_continuous(labels = percent_format(accuracy = 1)) +
    scale_x_date(date_labels = "%Y", date_breaks = "1 year") +
    labs(title = "Динамика индекса физического объёма экспорта, 2020–2026",
         x = "PERIOD", y = "fizob") +
    theme_viz(base_size = 14) +
    theme(plot.title = element_text(size = 15))
}

p_title_after <- function(fo) {
  ind <- fo |> filter(STRANA == "IN")
  lastp <- ind |> filter(PERIOD == max(PERIOD))
  ggplot(fo, aes(PERIOD, fizob, group = series)) +
    geom_hline(yintercept = 0, colour = viz$ink, linewidth = 0.4) +
    geom_line(data = filter(fo, STRANA != "IN"),
              colour = viz$grey, alpha = 0.55, linewidth = 0.7) +
    geom_line(data = ind, colour = viz$red, linewidth = 1.4) +
    geom_point(data = lastp, colour = viz$red, size = 2.6) +
    annotate("text", x = max(fo$PERIOD), y = lastp$fizob - 0.35,
             label = paste0("Индия\n", percent(lastp$fizob, accuracy = 0.1)),
             hjust = 1, vjust = 1, colour = viz$red, lineheight = 0.95,
             fontface = "bold", size = 4.2, family = VIZ_FONT) +
    scale_y_continuous(labels = percent_format(accuracy = 1)) +
    scale_x_date(date_labels = "%Y", date_breaks = "1 year") +
    labs(
      title = "Бум экспорта в Индию закончился: объёмы сокращаются второй год",
      subtitle = "Изменение физического объёма экспорта год к году · Китай и Турция — серым",
      x = NULL, y = NULL,
      caption = "Тот же график. Изменились только заголовок, цвет и подпись."
    ) +
    theme_viz(base_size = 14)
}

# =========================================================================
# КЕЙС 7. Как честно показать то, чего ещё нет (наукаст)
# =========================================================================
p_nowcast <- function(d, napr = "ЭК") {
  h <- d$headline |> filter(NAPR == napr, PERIOD >= as.Date("2024-01-01"))
  cut <- min(h$PERIOD[h$nowcast_share > 0], na.rm = TRUE)
  fact <- h |> filter(PERIOD <= cut) |> transmute(PERIOD, value = stoim_bn_fact)
  pred <- h |> filter(PERIOD >= cut) |> transmute(PERIOD, value = stoim_bn)
  top <- max(h$stoim_bn, na.rm = TRUE) * 1.14

  ggplot() +
    annotate("rect", xmin = cut, xmax = max(h$PERIOD) + 20, ymin = -Inf, ymax = Inf,
             fill = viz$orange, alpha = 0.10) +
    geom_line(data = fact, aes(PERIOD, value), colour = viz$navy, linewidth = 1.2) +
    geom_line(data = pred, aes(PERIOD, value), colour = viz$orange,
              linewidth = 1.3, linetype = "21") +
    geom_point(data = tail(pred, 1), aes(PERIOD, value), colour = viz$orange, size = 2.6) +
    annotate("text", x = cut - 25, y = top, label = "отчётные данные",
             hjust = 1, vjust = 1, colour = viz$navy, size = 4.1, family = VIZ_FONT) +
    annotate("text", x = cut + 25, y = top, label = "наукаст",
             hjust = 0, vjust = 1, colour = "#a06b0c", fontface = "bold",
             size = 4.1, family = VIZ_FONT) +
    scale_y_continuous(limits = c(0, top), expand = expansion(mult = c(0, 0.02)),
                       labels = label_number()) +
    scale_x_date(date_labels = "%m.%y", date_breaks = "3 months",
                 expand = expansion(mult = c(0.02, 0.04))) +
    labs(
      title = "Последние три месяца — не факт, а оценка, и это видно",
      subtitle = "Экспорт по месяцам, млрд $: сплошная линия — отчётные данные, пунктир — наукаст",
      x = NULL, y = "млрд $",
      caption = "Прогноз нельзя рисовать той же линией, что и факт: читатель не обязан догадываться"
    ) +
    theme_viz()
}

# =========================================================================
# ПРИМЕР. Результат запроса, нарисованный без дополнительной обработки
# =========================================================================
p_example_cn_export <- function(ex) {
  lastp <- ex[which.max(ex$PERIOD), ]
  ggplot(ex, aes(PERIOD, export_bn)) +
    geom_line(linewidth = 1.2, colour = viz$navy) +
    geom_point(data = lastp, size = 2.6, colour = viz$navy) +
    scale_x_date(date_labels = "%Y", date_breaks = "1 year") +
    scale_y_continuous(limits = c(0, NA), expand = expansion(mult = c(0, 0.08)),
                       labels = label_number()) +
    labs(
      title = "Экспорт в Китай: около 13 млрд $ в месяц",
      x = NULL, y = "млрд $"
    ) +
    theme_viz(base_size = 13)
}
