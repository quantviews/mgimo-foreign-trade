# Load the built-in foreign package
library(foreign)

# Read the DBF file
edizm_meta <- read.dbf("metadata/EDIZM.dbf")
edizm_meta <- read.csv('metadata/edizm.csv')

# View the first few rows

# Импорт

df_ind <- arrow::read_parquet('data_comparison/data_tidy/comparison_data_ind.parquet') %>%
  mutate(NETTO = as.numeric(NETTO),
         KOL = as.numeric(KOL)
  )
data_mirror_ind %>%
  is.na() %>%
  colSums()
  

# 0. Пропуски и странности
  
df_ind %>% 
  filter(Source == 'MIRROR') %>%
  count(EDIZM) %>%
  arrange(-n)

# Пропуски - нет пропусков

df_ind %>%
  filter(Source == 'MIRROR') %>%
  is.na() %>%
  colSums()

# Сравнение с Росстатом и comtrade

df_ind_comp <- df_ind %>%
  filter(PERIOD >= as.Date('2021-01-01'),
         PERIOD <= as.Date('2022-01-01')) %>%
  group_by(TNVED, PERIOD, NAPR, Source) %>%
  summarise(STOIM = sum(STOIM, na.rm = TRUE), .groups = "drop") %>%
  ungroup()

df_ind_comp %>%
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

ggsave('data_comparison/images/ind_agg.png')