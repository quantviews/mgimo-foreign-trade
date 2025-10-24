setwd('~/MGIMO-FT/')

comtrade_files <- list.files('data_raw/comtrade_data/')

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
           partner2Code,
           classificationCode,
           mosCode
    ) %>%
    filter(motCode == 0,
           customsCode == "C00",
           partner2Code == 0
           ) %>%
    mutate(NAPR = as.character(NAPR),
           EDIZM = as.character(EDIZM))
}

comtrade <- bind_rows(comtrade) # Дяже если ставить фильтр H6 всё равно будут 2-значные и 4-значные коды, посмотрите сами. Дальше мы фильтруем по числу символов.

# Пр

comtrade %>%
  filter(nchar(TNVED) == 6) %>% # Важно, чтобы исключить задвоения
  mutate(year = as.character(PERIOD) %>% substr(start = 1, stop = 4)) %>%
  group_by(NAPR, year) %>%
  summarize(STOIM = sum(STOIM) / 10^9)

# Проверка задвоений:

comtrade %>%
  filter(PERIOD == 202301,
         NAPR == 'X',
         nchar(TNVED) == 6) %>% 
  nrow()

comtrade %>%
  filter(PERIOD == 202301,
         NAPR == 'X',
         nchar(TNVED) == 6) %>%
  group_by(STRANA_1, TNVED) %>%
  summarize(STOIM = last(STOIM)) %>%
  nrow()
