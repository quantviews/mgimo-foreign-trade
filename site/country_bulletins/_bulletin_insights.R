# Shared text for HTML and PDF. All figures come from the rendered snapshots.
balance_previous <- trade_balance %>%
  filter(NAPR == "ТБ", PERIOD == date_max %m-% months(1)) %>% pull(STOIM)
balance_delta <- if (length(balance_previous) == 1) hl_balance - balance_previous / 1e9 else NA_real_
conclusion <- if (is.finite(balance_delta)) {
  paste0("По сравнению с предыдущим месяцем сальдо ",
         if (balance_delta >= 0) "увеличилось" else "сократилось",
         " на ", fmt_num(abs(balance_delta), 2), " млрд долл. ",
         "Экспорт ", if (hl_exp$stoim_mom >= 0) "вырос" else "снизился",
         ", импорт ", if (hl_imp$stoim_mom >= 0) "вырос." else "снизился.")
} else "Динамика стоимости и товарная структура приведены ниже."

fizob_note <- if (all(is.finite(c(hl_exp$fo_yoy, hl_imp$fo_yoy)))) {
  paste0("Индекс физобъёма: экспорт ", fmt_num(100 * hl_exp$fo_yoy),
         " процента г/г; импорт ", fmt_num(100 * hl_imp$fo_yoy), " процента г/г.")
} else "Полный индекс физобъёма за месяц не опубликован: есть пропуски в исходных рядах."
oil_note <- if (is.finite(oil_last)) {
  paste0("Масса поставок по показанному нефтяному коду: ", fmt_num(oil_last, 2), " млн т за месяц.")
} else "Полная масса поставок по нефтяному коду за месяц не установлена."
source_note <- if (all(c(hl_exp$nowcast_share, hl_imp$nowcast_share) == 0)) {
  "За текущий месяц использованы фактические записи статистики."
} else if (all(c(hl_exp$nowcast_share, hl_imp$nowcast_share) == 1)) {
  "Все показатели текущего месяца - оперативная оценка проекта (nowcast)."
} else paste0("Доля nowcast в стоимости месяца: экспорт ",
                     fmt_num(100 * hl_exp$nowcast_share),
                     ", импорт ", fmt_num(100 * hl_imp$nowcast_share), " процента.")
comparison_window <- paste0(format(date_max %m-% months(11), "%m.%Y"), " - ",
                            format(date_max, "%m.%Y"), " против ",
                            format(date_max %m-% months(23), "%m.%Y"), " - ",
                            format(date_max %m-% months(12), "%m.%Y"))
