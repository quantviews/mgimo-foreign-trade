# =============================================================================
# Индексы физического объёма и средних цен внешней торговли
# Версия (б): цепной индекс Фишера по скользящим годовым окнам
# -----------------------------------------------------------------------------
# Схема:
#   1. Для каждого ряда (страна × направление × 10 знаков) и месяца t берётся
#      окно 12 мес., заканчивающееся t, и сравнивается с прошлым календарным годом
#      (окном, заканчивающимся в декабре прошлого года).
#   2. В таком звене у ряда три числа: стоимость прошлого года v0, стоимость окна v
#      и индекс цены r = цена окна / цена прошлого года. Единицы измерения
#      сокращаются внутри r, поэтому кг и штуки агрегируются без пересчёта.
#      Измеритель (доп. единица или нетто) выбирается для каждого звена отдельно.
#   3. Если r не наблюдается (ряд новый, выбыл, нет объёма) или r – выброс,
#      берётся медиана r по той же субпозиции (6 знаков) во всех странах.
#      Тогда новый ряд даёт прирост объёма, выбывший – снижение.
#   4. Индекс Фишера любой группы рядов считается по четырём суммам:
#      A = Σv0, B = Σv, C = Σv0·r, D = Σv/r. Суммы складываются, поэтому 6, 4, 2 знака,
#      ALL и итоги по стране получаются суммированием одной таблицы.
#   5. Годовые звенья (декабрь к декабрю) перемножаются: цепной индекс к базовому году.
#      Внутри года месяц сравнивается с прошлым декабрём. Цепь сцепляется раз в год,
#      а не каждый месяц, чтобы не накапливался дрейф цепного индекса.
# Итог: стоимость = цена × объём точно, веса обновляются ежегодно, сезонности нет.
# Значения начинаются с декабря 2019 г. – первого полного окна.
# =============================================================================

library(tidyverse)
library(slider)
library(duckdb)

cov_min  <- 0.8 # измеритель годится, если он указан для ≥ 80% стоимости окна
uv_bound <- 3   # r – выброс, если отличается от медианы по субпозиции больше чем в 3 раза
n_min    <- 3   # минимум рядов для медианы на уровне 6, 4 и 2 знаков

con <- dbConnect(
   duckdb::duckdb(),
   "~/MGIMO-FT/db/unified_trade_data.duckdb",
   read_only = TRUE
)

#----------------------------------------
# Шаг 1. Исходные данные и панель -------
#----------------------------------------

raw <-
   dbGetQuery(con, "
    SELECT STRANA, NAPR, TNVED, PERIOD, EDIZM, STOIM, NETTO, KOL
    FROM unified_trade_data
  ") %>%
   mutate(PERIOD = as_date(PERIOD),
          across(c(STOIM, NETTO, KOL), ~ coalesce(.x, 0))) %>%
   filter(any(STOIM > 0), .by = c(STRANA, NAPR, TNVED)) # убираем ряды, где стоимость всегда 0 (Индия)

dbDisconnect(con, shutdown = TRUE)

period_min <- min(raw$PERIOD) # 2019-01-01
period_max <- max(raw$PERIOD)

# Доп. единицу можно использовать, если у ряда одна известная единица за весь период
kol_table <-
   raw %>%
   summarise(edizm = first(EDIZM), n_edizm = n_distinct(EDIZM), .by = c(STRANA, NAPR, TNVED)) %>%
   mutate(kol_ok = n_edizm == 1 & !is.na(edizm) & !(edizm %in% c('?', 'NA'))) %>%
   select(STRANA, NAPR, TNVED, kol_ok)

df <-
   raw %>%
   summarise(across(c(STOIM, NETTO, KOL), sum), .by = c(STRANA, NAPR, TNVED, PERIOD)) %>% # дубли ключа
   right_join(
      raw %>%
         distinct(STRANA, NAPR, TNVED) %>%
         cross_join(tibble(PERIOD = seq.Date(period_min, period_max, by = 'month'))),
      by = c('STRANA', 'NAPR', 'TNVED', 'PERIOD')
   ) %>%
   mutate(across(c(STOIM, NETTO, KOL), ~ coalesce(.x, 0))) %>%
   left_join(kol_table, by = c('STRANA', 'NAPR', 'TNVED')) %>%
   arrange(STRANA, NAPR, TNVED, PERIOD) # внутри ряда месяцы должны идти по порядку

rm(raw)

#----------------------------------------
# Шаг 2. Окна и индексы цен рядов -------
#----------------------------------------

links <-
   df %>%
   # Месяцы, где вместе со стоимостью указан объём
   mutate(ok_k = STOIM > 0 & KOL > 0,
          ok_n = STOIM > 0 & NETTO > 0) %>%
   # Суммы за 12 мес. (текущий и 11 предыдущих); неполное окно – NA
   mutate(
      v  = slide_sum(STOIM,        before = 11, complete = TRUE),
      vk = slide_sum(STOIM * ok_k, before = 11, complete = TRUE), # стоимость месяцев с KOL
      k  = slide_sum(KOL * ok_k,   before = 11, complete = TRUE),
      vn = slide_sum(STOIM * ok_n, before = 11, complete = TRUE), # стоимость месяцев с NETTO
      n  = slide_sum(NETTO * ok_n, before = 11, complete = TRUE),
      .by = c(STRANA, NAPR, TNVED)
   ) %>%
   filter(!is.na(v)) %>% # с декабря 2019 г.
   # Те же суммы за прошлый календарный год – окно, заканчивающееся в декабре прошлого года
   mutate(ref = floor_date(PERIOD, unit = 'years') - months(1)) %>%
   mutate(across(c(v, vk, k, vn, n), ~ .x[match(ref, PERIOD)], .names = '{.col}0'),
          .by = c(STRANA, NAPR, TNVED)) %>%
   mutate(v0 = coalesce(v0, 0)) %>% # для декабря 2019 г. прошлого года в данных нет
   filter(v > 0 | v0 > 0) %>%       # ряд без торговли в обоих окнах в звено не входит
   # Индекс цены ряда, в логарифмах. Доп. единица – если она указана для ≥ cov_min
   # стоимости в обоих окнах, иначе нетто, иначе NA.
   # Измеритель выбирается для каждого звена: один месяц без KOL больше не переводит
   # весь ряд на нетто навсегда, а смена единицы не создаёт скачка.
   mutate(lr = case_when(
      !(v > 0 & v0 > 0) ~ NA_real_, # ряд новый или выбыл
      kol_ok & vk >= cov_min * v & vk0 >= cov_min * v0 ~ log(vk / k) - log(vk0 / k0),
      vn >= cov_min * v & vn0 >= cov_min * v0          ~ log(vn / n) - log(vn0 / n0)
   )) %>%
   select(STRANA, NAPR, TNVED, PERIOD, v0, v, lr)

rm(df)

#----------------------------------------
# Шаг 3. Выбросы и вменение цен ---------
#----------------------------------------

# Ориентир – медиана индексов цен по той же субпозиции (6 знаков) во всех странах;
# если рядов меньше n_min – по позиции (4), группе (2), всем товарам направления.
links <-
   links %>%
   mutate(TNVED6 = substr(TNVED, start = 1, stop = 6),
          TNVED4 = substr(TNVED, start = 1, stop = 4),
          TNVED2 = substr(TNVED, start = 1, stop = 2)) %>%
   mutate(m6 = median(lr, na.rm = TRUE), n6 = sum(!is.na(lr)), .by = c(NAPR, TNVED6, PERIOD)) %>%
   mutate(m4 = median(lr, na.rm = TRUE), n4 = sum(!is.na(lr)), .by = c(NAPR, TNVED4, PERIOD)) %>%
   mutate(m2 = median(lr, na.rm = TRUE), n2 = sum(!is.na(lr)), .by = c(NAPR, TNVED2, PERIOD)) %>%
   mutate(m0 = median(lr, na.rm = TRUE), .by = c(NAPR, PERIOD)) %>%
   mutate(
      lr_med = case_when(n6 >= n_min ~ m6, n4 >= n_min ~ m4, n2 >= n_min ~ m2, .default = coalesce(m0, 0)),
      lr     = if_else(abs(lr - lr_med) > log(uv_bound), NA_real_, lr), # выброс → вменяем
      r      = exp(coalesce(lr, lr_med)),
      # Слагаемые индекса Фишера
      A = v0,
      B = v,
      C = v0 * r,
      D = v / r,
      I = if_else(is.na(lr), v0 + v, 0) # стоимость с вменённой ценой – для контроля качества
   ) %>%
   select(STRANA, NAPR, TNVED6, PERIOD, A, B, C, D, I)

#----------------------------------------
# Шаг 4. Суммы по группам ---------------
#----------------------------------------

# 6 знаков по странам и ALL. Дальше каждый уровень – сумма предыдущего.
s6 <-
   links %>%
   summarise(across(c(A, B, C, D, I), sum), .by = c(STRANA, NAPR, TNVED6, PERIOD)) %>%
   bind_rows(., summarise(., across(c(A, B, C, D, I), sum), .by = c(NAPR, TNVED6, PERIOD)) %>%
                mutate(STRANA = 'ALL')) %>%
   rename(TNVED_G = TNVED6) %>%
   mutate(level = 6)

rm(links)

s4 <-
   s6 %>%
   mutate(TNVED_G = substr(TNVED_G, start = 1, stop = 4), level = 4) %>%
   summarise(across(c(A, B, C, D, I), sum), .by = c(STRANA, NAPR, level, TNVED_G, PERIOD))

s2 <-
   s4 %>%
   mutate(TNVED_G = substr(TNVED_G, start = 1, stop = 2), level = 2) %>%
   summarise(across(c(A, B, C, D, I), sum), .by = c(STRANA, NAPR, level, TNVED_G, PERIOD))

s0 <-
   s2 %>%
   mutate(TNVED_G = '0', level = 0) %>% # итог по стране
   summarise(across(c(A, B, C, D, I), sum), .by = c(STRANA, NAPR, level, TNVED_G, PERIOD))

#----------------------------------------
# Шаг 5. Звенья и цепные индексы --------
#----------------------------------------

fo <-
   bind_rows(s6, s4, s2, s0) %>%
   # Месяцы без торговли в обоих окнах в суммах отсутствуют – возвращаем их нулями
   complete(nesting(STRANA, NAPR, level, TNVED_G), PERIOD,
            fill = list(A = 0, B = 0, C = 0, D = 0, I = 0)) %>%
   arrange(STRANA, NAPR, level, TNVED_G, PERIOD) %>%
   # Всё, что можно, считается вне группировки: так быстрее
   mutate(
      dec = month(PERIOD) == 12,
      # Звено «окно t к прошлому календарному году», формула Фишера:
      #   цены  = √(Ласпейрес × Пааше) = √(C/A × B/D),
      #   объём = √(Ласпейрес × Пааше) = √(D/A × B/C), цены × объём = B/A.
      q_link = case_when(B == 0 ~ 0, A > 0 ~ sqrt(D / A * B / C)), # нет торговли в окне – объём 0
      p_link = if_else(A > 0 & B > 0, sqrt(C / A * B / D), NA_real_),
      # Начало цепи – декабрь, когда торговля есть, а в прошлом году её не было.
      # Для групп, торговавших в 2019 г., это декабрь 2019 г. После года без торговли
      # звено посчитать нельзя, и цепь начинается заново со своей базой.
      start = dec & B > 0 & A == 0,
      # Годовые звенья (декабрь к декабрю); в остальные месяцы – 1
      q_dec = if_else(dec & !start, q_link, 1),
      p_dec = if_else(dec & !start, p_link, 1),
      yr    = year(PERIOD)
   ) %>%
   mutate(seg = cumsum(start), .by = c(STRANA, NAPR, level, TNVED_G)) %>%
   filter(seg > 0) %>% # до начала цепи база не определена
   mutate(
      # Произведение годовых звеньев от начала цепи до прошлого декабря
      q_chain = lag(cumprod(q_dec), default = 1),
      p_chain = lag(cumprod(p_dec), default = 1),
      bp      = min(yr), # базовый год – год начала цепи
      .by = c(STRANA, NAPR, level, TNVED_G, seg)
   ) %>%
   mutate(
      fizob    = if_else(start, 1, q_chain * q_link),
      price    = if_else(start, 1, p_chain * p_link),
      bp       = make_date(bp),
      stoim_12 = B,                                  # стоимость за 12 мес., долл.
      imp      = if_else(start, NA_real_, I / (A + B)) # доля стоимости с вменённым индексом цены
   ) %>%
   select(STRANA, NAPR, level, TNVED_G, PERIOD, fizob, price, bp, stoim_12, imp)

rm(s6, s4, s2, s0)

# Проверка, что всё верно: индекс стоимости = индекс цен × индекс физобъёма
fo %>%
   mutate(v_index = stoim_12 / first(stoim_12), .by = c(STRANA, NAPR, level, TNVED_G, bp)) %>%
   filter(abs(fizob * price - v_index) > 1e-9 * v_index)

#---------------------------------------------------------------
# Таблицы для 2, 4 и 6 знака и итог (ALL – строки STRANA = 'ALL')
#---------------------------------------------------------------

fo_2 <- fo %>% filter(level == 2) %>%
   select(STRANA, NAPR, TNVED2 = TNVED_G, PERIOD, fizob2 = fizob, price2 = price, bp, stoim_12, imp)
fo_4 <- fo %>% filter(level == 4) %>%
   select(STRANA, NAPR, TNVED4 = TNVED_G, PERIOD, fizob4 = fizob, price4 = price, bp, stoim_12, imp)
fo_6 <- fo %>% filter(level == 6) %>%
   select(STRANA, NAPR, TNVED6 = TNVED_G, PERIOD, fizob6 = fizob, price6 = price, bp, stoim_12, imp)
fo_tot <- fo %>% filter(level == 0) %>%
   select(STRANA, NAPR, PERIOD, fizob, price, bp, stoim_12, imp)

#------------------------------------------
# Сохранение результатов в parquet файлах -
#------------------------------------------

fo_2 %>% arrow::write_parquet('data_processed/fizob_2.parquet')
fo_4 %>% arrow::write_parquet('data_processed/fizob_4.parquet')
fo_6 %>% arrow::write_parquet('data_processed/fizob_6.parquet')
fo_tot %>% mutate(TNVED2 = '0') %>% arrow::write_parquet('data_processed/fizob_total.parquet')
