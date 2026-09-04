# libs

library(tidyverse)
library(arrow)

# wd

setwd("D:/Работа/mgimo-foreign-trade")
source('data_comparison/functions_comparison.R')

#

data_mirror_tur <- read_parquet('data_processed/tr_full.parquet') %>%
  mutate(PERIOD = as.Date(PERIOD),
         STRANA = 'TUR') %>%
  distinct()
  
fts_files <- list.files('data_raw/fts_data')
fts <- list()
for (i in 1:length(fts_files)){
  fts[[i]] <- read.csv(paste0('data_raw/fts_data/', fts_files[i])) %>% mutate(PERIOD = fts_files[i])
}

fts <- bind_rows(fts) %>%
  filter(STRANA == "TR") %>%
  mutate(PERIOD = str_remove(PERIOD, "\\.csv$") %>% ym(),
         STRANA = if_else(STRANA == 'TR', 'TUR', STRANA)
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

comtrade[[13]] %>%
  filter(TNVED == '854800', 
         NAPR == 'M')

comtrade[[13]] %>%
  filter(nchar(TNVED) == 6,
         STRANA_1 == 792) %>%
  count(TNVED) %>%
  arrange(-n)

read_parquet('data_raw/comtrade_data/2022-01.parquet') %>%
  filter(reporterCode == 792) %>%
  filter(cmdCode =='854800') %>%
  filter(motCode == 0,
         customsCode == "C00",
         flowCode == 'M') %>%
  view()
  

comtrade[[13]] %>%
  #filter(TNVED == '713200') %>%
  mutate(nchar_tnved = nchar(TNVED))

comtrade <- bind_rows(comtrade) %>%
  left_join(ct_codes %>% rename('STRANA_1' = 'M49 code',
                                'STRANA' = 'ISO-alpha3 code'), by = 'STRANA_1') %>%
  select(-c(STRANA_1, STRANA_2, `Country or Area`, `isReported`, `motCode`, `customsCode`, `partner2Code`)) %>%
  filter(
    nchar(TNVED) == 6,
    STRANA == 'TUR'
  ) %>%
  mutate(PERIOD = as.Date(paste0(PERIOD, "01"), format = "%Y%m%d"),
         NAPR = case_when(
           NAPR == 'X' ~ 'ИМ',.default = 'ЭК'
         )
  ) %>%
  mutate(Source = 'Comtrade',
         TNVED = as.character(TNVED)) %>%
  distinct()

comtrade %>% filter(NAPR == 'ЭК') %>%
  filter(PERIOD == as.Date('2022-01-01'))

# Build tidy df

df_6 <- fts %>%
  mutate(Source = 'FTS') %>%
  distinct() %>%
  rbind(comtrade) %>%
  rbind(data_mirror_tur %>%
          select(-c(TNVED4, TNVED2, TNVED, EDIZM_ISO)) %>%
          rename('TNVED' = 'TNVED6') %>%
          mutate(Source = 'MIRROR',
                 STRANA = if_else(STRANA == 'TR', 'TUR', STRANA))
  )

# Saving

df_6 %>% arrow::write_parquet('data_comparison/data_tidy/comparison_data_tur.parquet')
