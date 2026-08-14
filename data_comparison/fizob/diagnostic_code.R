# Проверка, что база обновилась

file.info('~/MGIMO-FT/db/unified_trade_data.duckdb')

# Environment

library(tidyverse)
library(patchwork)
library(duckdb)
library(arrow)

con <- dbConnect(
  duckdb::duckdb(),
  "~/MGIMO-FT/db/unified_trade_data.duckdb",
  read_only = TRUE
)

dbGetQuery(con, "SHOW TABLES")

# Импорт и экспорт - разные графики.

dbGetQuery(con, "
  SELECT *
  FROM fizob_index_v
  WHERE STRANA = 'ALL'
    AND tn_code = '99'
") %>%
  ggplot(aes(x = PERIOD, y = fizob)) +
  geom_line() +
  facet_wrap(~NAPR)

# Ошибка только в ALL

dbGetQuery(con, "
  SELECT *
  FROM fizob_index_v
  WHERE tn_code = '99'
") %>%
  filter(any(fizob > 100),
         .by = c('STRANA', 'NAPR', 'tn_code'))

# Мои исходные данные

read_parquet('~/MGIMO-FT/data_processed/fizob_2.parquet') %>%
  filter(any(fizob2 > 100),
         .by = c('STRANA', 'NAPR', 'TNVED2')
         ) %>%
  filter(
    fizob2 == max(fizob2),
    .by = c('STRANA', 'NAPR', 'TNVED2')
  )

# Смотрю, почему возник выборс по ценам.

data_fo_summary <- 
  data_fo_all %>%
  mutate(year = floor_date(PERIOD, unit = 'year')) %>%
  filter(year %in% as_date(c('2019-01-01', '2020-01-01', '2021-01-01'))) %>%
  group_by(NAPR, TNVED, year) %>%
  reframe(mean_price = mean(price_12),
          mean_kol = mean(kol_12),
          mean_netto = mean(netto_12),
          mean_stoim = mean(stoim_12),
          mean_share2 = mean(share_TNVED2),
          mean_share4 = mean(share_TNVED4),
          mean_share6 = mean(share_TNVED6)
          )

data_fo_summary %>%
  filter(year %in% as_date(c("2019-01-01", "2020-01-01"))) %>%
  pivot_wider(
    names_from = year,
    values_from = mean_stoim,
    names_prefix = "stoim_"
  ) %>%
  mutate(diff_stoim = `stoim_2020-01-01` - `stoim_2019-01-01`) %>%
  arran

data_fo_summary %>% arrange(TNVED) %>% View()

data_fo %>%
  filter(STRANA == 'AD', NAPR == 'ИМ', TNVED == '0409000000') %>%
  View()

data_fo %>%
  filter(TNVED2 == '99') %>%
  filter(stoim_12 > 0) %>%
  filter(PERIOD == as_date('2020-01-01'))

data_fo %>%
  filter(TNVED2 == '99') %>%
  group_by(NAPR, PERIOD) %>%
  reframe(stoim = sum(STOIM)) %>%
  ggplot(aes(x = PERIOD, y = stoim)) +
  geom_line() +
  facet_wrap(~NAPR)

data_fo %>%
  filter(TNVED2 == '99',
         NAPR == 'ЭК') %>%
  mutate(STRANA_TNVED = paste0(STRANA, '_', TNVED)) %>%
  ggplot(aes(x = PERIOD, y = STOIM)) +
  geom_line() +
  facet_wrap(~STRANA_TNVED)


data_fo_summary %>%
  group_by(NAPR, TNVED) %>%
  reframe(netto_diff_2020 = mean_netto[year == as_date('2020-01-01')] / mean_netto[year = as_date('2019-01-01')],
          share_2019 = share_2019,
          share_2020 = share_2020
  )
        )

data_fo_all %>%
  filter(PERIOD == as_date('2020-01-01')) %>%
  arrange(-share_TNVED2) %>%
  filter(TNVED2 == '99') %>% 
  View()

data_fo_summary %>%
  filter(NAPR == 'ИМ',
         TNVED == '8517121000')

result %>%
  arrange(-stoim_diff_2020)

# Только у Турции и Китая данные есть и STOIM и NETTO в группе 99

dbGetQuery(con, "
  SELECT STRANA, NAPR, TNVED, PERIOD, EDIZM, STOIM, NETTO, KOL
  FROM unified_trade_data
") %>%
  filter(any(STOIM > 0), .by = c(STRANA, NAPR, TNVED)) %>%
  filter(TNVED %>% str_starts('99') ) %>%
  filter(NETTO > 0)

dbGetQuery(con, "
  SELECT STRANA, NAPR, TNVED, PERIOD, EDIZM, STOIM, NETTO, KOL
  FROM unified_trade_data
") %>%
  filter(any(STOIM > 0), .by = c(STRANA, NAPR, TNVED)) %>%
  filter(STOIM > 0,
         NETTO == 0) %>%
  arrange(TNVED) %>%
  View()

# Нашёл причину.
# При создании df_complete я "воссоздавал значения", если STOIM > 0 а KOL или NETTO == 0. Для этого я использовал цену price_12.
# Важно! Здесь я ЗАМЕНЯЮ старые KOL и NETTO, Если STOIM > 0, а KOL и NETTO == 0. Замены происходят в редких случаях, тем не менее, дальше KOL и NETTO - не исходные.


#mutate(
#  KOL = if_else( 
#    (STOIM > 0) & !(KOL > 0),
#    STOIM / price_12,
#    KOL),
#  NETTO = if_else(
#    (STOIM > 0) & !(NETTO > 0),
#    STOIM / price_12,
#    NETTO
#  )

# Тогда давайте проверим price_12 во всех табличках.

data_fo %>%
  filter(TNVED %>% str_starts('99')) %>%
  mutate(STRANA_TNVED_NAPR = paste0(STRANA, '_', TNVED, '_', NAPR)) %>%
  ggplot(aes(x = PERIOD, y = price_12)) +
  geom_line() +
  facet_wrap(~STRANA_TNVED_NAPR)

df_complete %>%
  filter(TNVED %>% str_starts('99'))

dbGetQuery(con, "
  SELECT STRANA, NAPR, TNVED, PERIOD, EDIZM, STOIM, NETTO, KOL
  FROM unified_trade_data
") %>%
  filter(TNVED %>% str_starts('99'))

# Ряды пропадают при создании df_complete

dbGetQuery(con, "
  SELECT STRANA, NAPR, TNVED, PERIOD, EDIZM, STOIM, NETTO, KOL
  FROM unified_trade_data
") %>%
  filter(STRANA == 'TR', NAPR == 'ЭК', TNVED == '9919000000') %>%
  mutate(price = STOIM / NETTO)

df_complete %>%
  filter(STRANA == 'TR', NAPR == 'ЭК', TNVED == '9919000000')
  
# Нарисую получившиеся новые физобъёмы. Для 99% групп ничего не поменялось

p_new <-
  fo_2 %>%
  filter(STRANA == 'CN',
         TNVED2 == '50') %>%
  ggplot(aes(x = PERIOD, y = fizob2)) +
  geom_line() +
  facet_wrap(~NAPR)

p_old <- 
  dbGetQuery(con, "
  SELECT *
  FROM fizob_index_v
  WHERE STRANA = 'CN'
    AND tn_code = '50'
") %>%
  ggplot(aes(x = PERIOD, y = fizob)) +
  geom_line() +
  facet_wrap(~NAPR)

p_new / p_old

# Теперь для этео й терецкой группы.

p_new <-
  fo_4 %>%
  filter(STRANA == 'TR',
         TNVED4 == '9919') %>%
  ggplot(aes(x = PERIOD, y = fizob4)) +
  geom_line() +
  facet_wrap(~NAPR)

p_old <- 
  dbGetQuery(con, "
  SELECT *
  FROM fizob_index_v
  WHERE STRANA = 'TR'
    AND tn_code = '9919'
") %>%
  ggplot(aes(x = PERIOD, y = fizob)) +
  geom_line() +
  facet_wrap(~NAPR)

p_new / p_old

# Тут всё выглядит нормально

fo_2 %>%
  filter(STRANA == 'ALL', 
         TNVED2 == '99') %>%
  ggplot(aes(x = PERIOD, y = fizob2)) +
  geom_line() +
  facet_wrap(~NAPR)
         
