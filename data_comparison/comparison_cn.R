# libs

library(tidyverse)
library(arrow)

# wd

setwd("D:/Работа/mgimo-foreign-trade")
source('data_comparison/functions_comparison.R')

# import ch_full

ch_full <- read_parquet('data_processed/ch_full.parquet') %>%
  distinct()

# import fts

fts_files <- list.files('data_raw/fts_data')
fts <- list()
for (i in 1:length(fts_files)){
  fts[[i]] <- read.csv(paste0('data_raw/fts_data/', fts_files[i])) %>% mutate(PERIOD = fts_files[i])
}

fts <- bind_rows(fts) %>%
  filter(STRANA == "CN") %>%
  mutate(PERIOD = str_remove(PERIOD, "\\.csv$") %>% ym(),
         STRANA = if_else(STRANA == 'CN', 'CHN', STRANA)
         )

# import comtrade

comtrade_files <- list.files('data_raw/comtrade_data/')
ct_codes <- readxl::read_excel('data_comparison/references/country_codes.xlsx', sheet = 2)

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
             KOL = qty,
             motCode,
             isReported,
             motCode,
             customsCode,
             partner2Code
      ) %>%
      filter(motCode == 0,
             customsCode == "C00",
             partner2Code == 0) %>%
      mutate(NAPR = as.character(NAPR),
             EDIZM = as.character(EDIZM))
  }

comtrade <- bind_rows(comtrade) %>%
  left_join(ct_codes %>% rename('STRANA_1' = 'M49 code',
                                'STRANA' = 'ISO-alpha3 code'), by = 'STRANA_1') %>%
  select(-c(STRANA_1, STRANA_2, `Country or Area`, `motCode`, `isReported`, motCode, customsCode, partner2Code)) %>%
  filter(
    nchar(TNVED) == 6,
    STRANA == 'CHN'
  ) %>%
  mutate(PERIOD = as.Date(paste0(PERIOD, "01"), format = "%Y%m%d"),
         TNVED = as.numeric(TNVED),
         NAPR = case_when(
           NAPR == 'X' ~ 'ИМ',.default = 'ЭК'
         )
) %>%
  mutate(Source = 'Comtrade') %>%
  distinct()

# Build tidy df

df_6 <- fts %>%
  mutate(Source = 'FTS') %>%
  distinct() %>%
  rbind(comtrade) %>%
  rbind(ch_full %>%
          select(-c(TNVED4, TNVED2, TNVED)) %>%
          rename('TNVED' = 'TNVED6') %>%
          mutate(Source = 'MIRROR',
                 STRANA = if_else(STRANA == 'CN', 'CHN', STRANA))
        )

# Saving

df_6 %>% arrow::write_parquet('data_comparison/data_tidy/comparison_data_chn.parquet')
