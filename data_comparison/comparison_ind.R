# libs

library(tidyverse)
library(arrow)

# wd

setwd("D:/Работа/mgimo-foreign-trade")
source('data_comparison/functions_comparison.R')

#

data_mirror_ind <- read_parquet('data_processed/india_full.parquet') %>%
  mutate(PERIOD = as.Date(PERIOD),
         STRANA = 'IND')

fts_files <- list.files('data_raw/fts_data')
fts <- list()
for (i in 1:length(fts_files)){
  fts[[i]] <- read.csv(paste0('data_raw/fts_data/', fts_files[i])) %>% mutate(PERIOD = fts_files[i])
}

fts <- bind_rows(fts) %>%
  filter(STRANA == "IN") %>%
  mutate(PERIOD = str_remove(PERIOD, "\\.csv$") %>% ym(),
         STRANA = if_else(STRANA == 'IN', 'IND', STRANA)
  )

# import comtrade

comtrade_files <- list.files('data_raw/comtrade_data/')
ct_codes <- readxl::read_excel('data_comparison/references/country_codes.xlsx', sheet = 2)

read_parquet(
  paste0('data_raw/comtrade_data/',
         comtrade_files[i])
)

comtrade <- list()
for (i in 1:length(comtrade_files)){
  comtrade[[i]] <- read_parquet(
    paste0('data_raw/comtrade_data/',
           comtrade_files[i])
  ) %>%
    select(NAPR = flowCode,
           PERIOD = period,
           STRANA_1 = reporterCode,
           STRANA_2 = partnerCode,
           TNVED = cmdCode,
           EDIZM = qtyUnitAbbr,
           STOIM = primaryValue,
           NETTO = netWgt,
           KOL = qty
    ) %>%
    mutate(NAPR = as.character(NAPR),
           EDIZM = as.character(EDIZM))
}

comtrade <- bind_rows(comtrade) %>%
  left_join(ct_codes %>% rename('STRANA_1' = 'M49 code',
                                'STRANA' = 'ISO-alpha3 code'), by = 'STRANA_1') %>%
  select(-c(STRANA_1, STRANA_2, `Country or Area`)) %>%
  filter(
    nchar(TNVED) == 6,
    STRANA == 'IND'
  ) %>%
  mutate(PERIOD = as.Date(paste0(PERIOD, "01"), format = "%Y%m%d"),
         TNVED = as.numeric(TNVED),
         NAPR = case_when(
           NAPR == 'X' ~ 'ИМ',.default = 'ЭК'
         )
  ) %>%
  mutate(Source = 'Comtrade',
         TNVED = as.character(TNVED))

# Build tidy df

df_6 <- fts %>%
  mutate(Source = 'FTS') %>%
  rbind(comtrade) %>%
  rbind(data_mirror_ind %>%
          select(-c(TNVED4, TNVED2, TNVED, starts_with('NAME'))) %>%
          rename('TNVED' = 'TNVED6') %>%
          mutate(Source = 'MIRROR',
                 STRANA = if_else(STRANA == 'TR', 'TUR', STRANA),
                 TNVED = as.character(TNVED))
  )

# Saving

df_6 %>% arrow::write_parquet('data_comparison/data_tidy/comparison_data_ind.parquet')
