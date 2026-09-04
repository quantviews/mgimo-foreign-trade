# Load the built-in foreign package
library(foreign)
library(patchwork)

foptions(scipen = 999)

# Read the DBF file
edizm_meta <- read.dbf("metadata/EDIZM.dbf")
edizm_meta <- read.csv('metadata/edizm.csv')

# View the first few rows

# Импорт

df_tur <- arrow::read_parquet('data_comparison/data_tidy/comparison_data_tur.parquet') %>%
  mutate(NETTO = as.numeric(NETTO),
         KOL = as.numeric(KOL)
         )

data_mirror_tur <- read_parquet('data_processed/tr_full.parquet') %>%
  mutate(PERIOD = as.Date(PERIOD),
         STRANA = 'TUR')

# 0. Пропуски и странности

df_tur %>% 
  filter(Source == 'MIRROR') %>%
  count(EDIZM) %>%
  arrange(-n)

# Пропуски - нет пропусков

df_tur %>%
  filter(Source == 'MIRROR') %>%
  is.na() %>%
  colSums()

# Сравнение с Росстатом и comtrade

df_tur_comp <- df_tur %>%
  group_by(TNVED, PERIOD, NAPR, Source) %>%
  summarise(STOIM = sum(STOIM, na.rm = TRUE), .groups = "drop") %>%
  ungroup()

df_tur_comp %>%
  filter(PERIOD >= as.Date('2021-01-01'),
               PERIOD <= as.Date('2022-01-01')) %>%
  group_by(PERIOD, NAPR, Source) %>%
  summarise(
    total_stoim = sum(STOIM, na.rm = T) / 10^9
  ) %>%
  mutate(total_stoim = if_else(Source == 'Comtrade', total_stoim / 10, total_stoim)) %>%
  ggplot(aes(x = PERIOD, y = total_stoim, color = Source)) +
  geom_line() +
  geom_point(fill = 'white', shape = 21) +
  facet_wrap(~NAPR, nrow = 2, scale = 'free') +
  theme(legend.position = 'bottom',
        legend.title = element_blank()) +
  labs(x = NULL,
       y = 'Всего, трлн. долл.',
       title = 'Турция')

ggsave('data_comparison/images/tur_agg.png')

# Отклонения Зеркальной статистики от Comtrade. Разница на уровне 6-значных кодов

df_tur_comp %>%
  pivot_wider(
    id_cols = c(TNVED, PERIOD, NAPR),   # keep NAPR as an identifier
    names_from = Source,
    values_from = STOIM
  ) %>%
  group_by(NAPR, TNVED, PERIOD) %>%
  summarise(
    Comtrade_minus_Mirror = Comtrade - MIRROR
  ) %>% 
  na.omit() %>%
  arrange(-Comtrade_minus_Mirror) %>%
  arrow::write_parquet('data_comparison/output/tur_deviations.parquet')

df_tur_comp %>%
  filter(TNVED == '702000',
         NAPR == 'ИМ',
         PERIOD >= as.Date('2021-01-01')) %>%
  ggplot(aes(x = PERIOD, y = STOIM / 10^3, color = Source)) +
  geom_line() +
  geom_point() +
  labs(x = NULL, y = 'Всего, млн. долл.',
       title = 'Турция: экспорт изделия из стекла прочих: ТН ВЭД 702000',
       subtitle = 'Разница довольно заметная. Ряды совсем не похожи.')

ggsave('data_comparison/images/deviations_tur_1.png')

df_tur_comp %>%
  filter(TNVED == '702000',
         NAPR == 'ИМ') %>%
  ggplot(aes(x = PERIOD, y = STOIM / 10^3, color = Source)) +
  geom_line() +
  geom_point() +
  labs(x = NULL, y = 'Всего, млн. долл.',
       title = 'Турция: экспорт изделия из стекла прочих: ТН ВЭД 702000',
       subtitle = 'Тот же ряд, но больший масштаб')

ggsave('data_comparison/images/deviations_tur_2.png')

tur_pl_3_1 <- 
  df_tur_comp %>%
  filter(TNVED == '100199',
         NAPR == 'ЭК') %>%
  ggplot(aes(x = PERIOD, y = STOIM / 10^6, color = Source)) +
  geom_line() +
  geom_point() +
  labs(x = NULL, y = 'Всего, млрд. долл.',
       title = 'Турция: экспорт пшеницы прочей: ТН ВЭД 100199',
       subtitle = 'На этот раз Comtrade > Зеркальной статистики')

tur_pl_3_2 <-
df_tur_comp %>%
  filter(TNVED == '100199',
         NAPR == 'ЭК') %>%
  mutate(STOIM = if_else(Source == 'Comtrade', STOIM / 10, STOIM)) %>%
  ggplot(aes(x = PERIOD, y = STOIM / 10^6, color = Source)) +
  geom_line() +
  geom_point() +
  labs(x = NULL, y = 'Всего, млрд. долл.',
       title = 'Турция: экспорт пшеницы прочей: ТН ВЭД 100199',
       subtitle = 'То же самое, но данные Comtrade / 10')

tur_pl_3_1 / tur_pl_3_2

ggsave('data_comparison/images/deviations_tur_3.png')

df_tur_comp %>%
  filter(Source == 'MIRROR') %>%
  group_by(TNVED, NAPR) %>%
  reframe(arch_pvalue = arch_pvalue(STOIM)) %>%
  filter(arch_pvalue > 0.1)

ggsave('data_comparison/images/deviations_tur_2.png')

df_tur_comp %>%
  filter(TNVED == '740311',
         NAPR == 'ЭК') %>%
  ggplot(aes(x = PERIOD, y = STOIM / 10^3, color = Source)) +
  geom_line() +
  geom_point() +
  labs(x = NULL, y = 'Всего, млн. долл.',
       title = 'Турция: экспорт изделия из стекла прочих: ТН ВЭД 702000',
       subtitle = 'В Comtrade почему-то нет этого ряда. \n Также, вероятно, есть выбросы. Например, в феврале 2020. \n + пропуски с 2005 по 2010 гг.')

ggsave('data_comparison/images/deviations_tur_1.png')

# ?-ы в EDIZM

df_tur %>%
  filter(TNVED == '100119') %>%
  View()

tnved_q <- df_tur %>%
  filter(EDIZM == '?') %>%
  arrange(TNVED) %>%
  pull(TNVED) %>%
  unique()
  
df_tur %>%
  filter(Source == 'MIRROR') %>%
  group_by(NAPR, TNVED) %>%
  summarise(EDIZM_u = EDIZM %>% unique() %>% toString(), .groups = 'drop') %>% View()

# 259
df_tur %>%
  filter(Source == 'MIRROR') %>%
  group_by(TNVED, NAPR, Source) %>%
  summarise(
    n_edizm = n_distinct(na.omit(EDIZM)),
    EDIZM = paste(unique(na.omit(EDIZM)), collapse = ", "),
    STOIM = sum(STOIM, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  filter(n_edizm > 1)

df_tur %>%
  filter(Source != 'FTS') %>%
  group_by(TNVED, NAPR) %>%
  summarise(
    n_edizm = n_distinct(na.omit(EDIZM)),
    EDIZM = paste(unique(na.omit(EDIZM)), collapse = ", "),
    STOIM = sum(STOIM, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  filter(n_edizm > 1)

df_tur %>%
  filter(TNVED == '100199') %>%
  View()
