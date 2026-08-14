library(tidyverse)
library(duckdb)
library(arrow)
library(forecast)
library(plotly)
library(ggbreak)
library(ggtext)
library(Metrics) # для метрик качества (совершенно не обязательно)

# Для параллельных вычислений
library(furrr)
library(future)


con <- dbConnect(
   duckdb::duckdb(),
   "db/unified_trade_data.duckdb",
   read_only = TRUE
)

dbDisconnect(con, shutdown = TRUE) # На всякий случай оставлю тут

replace_outlier <- function(x, frequency = 12) {
   as.numeric(tsclean(ts(x, frequency = frequency)))
}

dbListFields(con, "fizob_index")

tab_stoim <- dbGetQuery(con, '
           SELECT STRANA, NAPR, PERIOD, STOIM
           FROM unified_trade_data') %>%
   reframe(
      STOIM = sum(STOIM, na.rm = T),
      .by = c(STRANA, NAPR, PERIOD)
   )

library(tidyverse)
library(duckdb)
library(arrow)

# Что мы тестируем
# 1. Полнота
# 2. Аутлаеры
# 2.1 изменение sd
# 2.2 изменение уровня
# По странам
# Отдельная страничка по наукасту

con <- dbConnect(
   duckdb::duckdb(),
   "~/MGIMO-FT/db/unified_trade_data.duckdb",
   read_only = TRUE
)

#dbDisconnect(con, shutdown = TRUE) # На всякий случай оставлю тут

# Comtrade

dbGetQuery(con, '
           SELECT STRANA, NAPR, PERIOD, STOIM, NETTO
           FROM unified_trade_data_enriched') %>%
   filter(STRANA %in% setdiff(STRANA, c('IN', 'TR', 'CN'))) %>%
   mutate(year = floor_date(PERIOD, unit = 'years')) %>%
   reframe(across(
      c(STOIM, NETTO),
      ~ sum(.x, na.rm = T)),
      .by = c(year, NAPR)
   ) %>%
   pivot_longer(STOIM:NETTO) %>%
   mutate(value = value / 10^9,
          panel = paste(name, NAPR, sep = " | ")
   ) %>%
   ggplot(aes(x = year, y = value)) +
   geom_line() +
   coord_cartesian(ylim = c(0, NA)) +
   facet_wrap(~panel, scales = 'free') +
   labs(y = 'млрд. ед.')

# 

dbGetQuery(con, '
           SELECT STRANA, NAPR, PERIOD, STOIM, NETTO
           FROM unified_trade_data_enriched') %>%
   filter(STRANA %in% c('IN', 'TR', 'CN')) %>%
   mutate(year = floor_date(PERIOD, unit = 'years')) %>%
   reframe(across(
      c(STOIM, NETTO),
      ~ sum(.x, na.rm = T)),
      .by = c(STRANA, NAPR, year)
   ) %>%
   pivot_longer(STOIM:NETTO) %>%
   mutate(value = value / 10^9,
          panel = paste(STRANA, name, NAPR, sep = " | ")
   ) %>%
   ggplot(aes(x = year, y = value)) +
   geom_line() +
   coord_cartesian(ylim = c(0, NA)) +
   facet_wrap(~panel, scales = 'free') +
   labs(y = 'млрд. ед.')

---
   title: "Определение выбросов"
author: "E. Tymchenko"
date: "2025-11-07"
output: html_document
---
   
   ## TLDR:
   
   * Мы не знаем (и никогда не узнаем) точно, содержит ли ряд выбросы на самом деле. Единственное, что мы можем - статистически выделить, что для отдельных временных рядов некоторые наблюдения выглядят "необычно".
* Необычно - это когда 1 и более наблюдений как будто происходят из другого статистического процесса, выглядят инородно.
* Наиболее простой для вычислений способ это сделать - использовать z-score для ряда, т.е. рассматривать ряд, как кросс-секшн и отбросить особенности временного ряда, как такогого, потому что временные ряды - сложные и гипотезы на них тестировать вычислительно очень долго.

```{r setup, include=FALSE}
knitr::opts_chunk$set(
   warning = FALSE,
   message = FALSE
)
```

Библиотеки

```{r}
library(tidyverse)
library(forecast) # модели для прогнозирования
library(microbenchmark) # для бенчмарков
library(duckdb) #~
library(Metrics) # для метрик качества (совершенно не обязательно)

# Для параллельных вычислений
library(furrr)
library(future)

#source('~/MGIMO-FT/data_comparison/functions_forecasting.R')
source('data_analytics/functions_outlier_detection.R')

steps_ahead = 1 # число периодов для прогнозов
```

Подключаемся к базе данных. Для демонстрации я буду использовать полную базу данных, чтобы ничего больше не нужно было дорабатывать, а только перевести.

```{r}
con <- con <- dbConnect(
   duckdb::duckdb(),
   "~/MGIMO-FT/db/unified_trade_data.duckdb",
   read_only = TRUE
)

dbGetQuery(con, "SHOW TABLES")
```

Теперь применим функции для поиска внутренних выбросов. Для определения выбросов я предлагаю 2 функции:
   
   `show_outliers` cчитает, сколько значений в векторе `x` выбиваются за рамки нормального разброса и превышают заданный порог `tv`.  
- `nsd` — насколько далеко от среднего считать выбросом.  
- `tv` — минимальное значение для проверки.  

`outlier_frac` считает выбросы для отношения двух векторов `x / y` (например, отношение KOL к NETTO). Выбросом считается значение, которое сильно отличается от среднего отношения и при этом числитель `x` больше порога `tv`.  

```{r}
# Эти значения я нашёл опытнчм путём, можете поэксперемениировать.
# Научно выверенных объективно правильных значений этих переменных всё рвано нет и быть не может.
tv <- 10^6
sd_num <- 6

all_outliers <- dbGetQuery(con, "
  SELECT STRANA, TNVED, PERIOD, NAPR, KOL, STOIM, NETTO
  FROM unified_trade_data
") %>%
   arrange(PERIOD) %>%
   group_by(STRANA, TNVED, NAPR) %>%
   reframe(outliers_1 = show_outliers(KOL, sd_num, tv),
           outliers_2 = outlier_frac(KOL, STOIM, sd_num, tv),
           outliers_3 = outlier_frac(KOL, NETTO, sd_num, tv)
   ) %>%
   # Фильтруем, что для 3 вариантов обнаружения были найдены выбросы
   filter(outliers_3 >= 1,
          outliers_2 >= 1,
          outliers_1 >= 1)
all_outliers
```

Для полученных рядов можно построить графики.

```{r}
dbGetQuery(con, "
  SELECT STRANA, TNVED, PERIOD, NAPR, KOL
  FROM unified_trade_data
") %>%
   semi_join(all_outliers, by = c('STRANA', 'TNVED', 'NAPR')) %>%
   mutate(group = paste(STRANA, TNVED, NAPR)) %>%
   ggplot(aes(x = PERIOD, y = KOL / 10^6)) +
   geom_line() +
   facet_wrap(~group, scales = 'free_y') +
   labs(x = NULL, y = 'KOL, млн')
```
Выглядит действительно похоже на выбросы, особенно экспорт 450 млн. бритв в Словению в середине 2022 г.