# Load the built-in foreign package
library(foreign)

# Read the DBF file
edizm_meta <- read.dbf("metadata/EDIZM.dbf")
edizm_meta <- read.csv('metadata/edizm.csv')

# View the first few rows

# Импорт

df_chn <- arrow::read_parquet('data_comparison/data_tidy/comparison_data_chn.parquet') %>%
  mutate(NETTO = as.numeric(NETTO),
         KOL = as.numeric(KOL))

df_chn_comp <- df_chn %>%
  #filter(PERIOD >= as.Date('2021-01-01'),
  #       PERIOD <= as.Date('2022-01-01')) %>%
  group_by(TNVED, PERIOD, NAPR, Source) %>%
  summarise(STOIM = sum(STOIM, na.rm = TRUE), .groups = "drop") %>%
  ungroup()

df_chn %>% 
  filter(Source == 'MIRROR') %>%
  count(EDIZM) %>%
  arrange(-n)

# Пропуски
df_chn %>%
  filter(Source == 'MIRROR') %>%
  is.na() %>%
  colSums()

df_chn %>%
  filter(Source == 'MIRROR') %>%
  filter(is.na(NETTO))

# Отклонения Зеркальной статистики от Comtrade. Разница на уровне 6-значных кодов

df_chn_comp %>%
  filter(TNVED == '740311',
         NAPR == 'ЭК') %>%
  ggplot(aes(x = PERIOD, y = STOIM, color = Source)) +
  geom_line() +
  geom_point() +
  labs(x = NULL, y = 'Всего, трлн. долл.',
       title = 'Китай: экспорт медной проволоки: ТН ВЭД 740311',
       subtitle = 'Разница в июне 2024 ~ 193 мдрд долл.')

ggsave('data_comparison/images/deviations_chn.png')

df_chn_comp %>%
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
  arrange(Comtrade_minus_Mirror) %>%
  filter(abs(Comtrade_minus_Mirror) > 10^6 ) %>%
  pull(TNVED) %>%
  unique()
  
  arrow::write_parquet('data_comparison/output/chn_deviations.parquet')


# Разница в общем импорте и экспорте.

df_chn_comp %>%
  filter(PERIOD >= as.Date('2021-01-01'),
               PERIOD <= as.Date('2022-01-01')) %>%
  group_by(PERIOD, NAPR, Source) %>%
    summarise(
    total_stoim = sum(STOIM, na.rm = T) / 10^9
  ) %>%
  ggplot(aes(x = PERIOD, y = total_stoim, color = Source)) +
  geom_line() +
  geom_point(fill = 'white', shape = 21) +
  facet_wrap(~NAPR, nrow = 2) +
  theme(legend.position = 'bottom',
        legend.title = element_blank()) +
  labs(x = NULL,
       y = 'Всего, трлн. долл.',
       title = 'Китай')
ggsave('data_comparison/images/chn_agg.png')