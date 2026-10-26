# Isolated matched-basket Fisher link, not a replacement for the full index.
# Rscript scripts/calculate_fisher_matched.R input.duckdb new-output-directory
source('src/fizob_fisher.R')
library(DBI); library(duckdb)
args <- commandArgs(trailingOnly=TRUE)
stopifnot(length(args)==2,!dir.exists(args[2]))
dir.create(args[2],recursive=TRUE)
con <- dbConnect(duckdb(),args[1],read_only=TRUE)
raw <- dbGetQuery(con,"SELECT STRANA,NAPR,TNVED,PERIOD,EDIZM,STOIM,NETTO,KOL,TYPE,SOURCE,KOL_ESTIMATED,NETTO_ESTIMATED
 FROM unified_trade_data WHERE TYPE='fact' AND SOURCE!='nowcast' AND STRANA!='ALL'
 AND PERIOD BETWEEN DATE '2025-01-01' AND DATE '2026-08-01'")
dbDisconnect(con,shutdown=TRUE)
raw$PERIOD <- as.Date(raw$PERIOD)
calendar <- seq.Date(as.Date('2025-01-01'),as.Date('2026-08-01'),by='month')
links <- bind_rows(lapply(split(raw,raw$STRANA),function(part)
  fisher_links(part,calendar,.8))) %>% filter(PERIOD==as.Date('2026-08-01'))
# Source consistency is required for the basket, not merely for a price donor.
sources <- raw %>% summarise(source_count=n_distinct(SOURCE),.by=c(STRANA,NAPR,TNVED))
links <- links %>% left_join(sources,by=c('STRANA','NAPR','TNVED')) %>%
  mutate(eligible=is.finite(v)&is.finite(v0)&source_count==1 & (v>0|v0>0))
donors <- links %>% filter(eligible,is.finite(lr))
for(level in c(6,4,2)) {
  med <- donors %>% mutate(code=substr(TNVED,1,level)) %>%
    summarise(m=median(lr),n=n(),.by=c(NAPR,code))
  names(med)[3:4] <- paste0(c('m','n'),level)
  links <- links %>% mutate(code=substr(TNVED,1,level)) %>% left_join(med,by=c('NAPR','code')) %>% select(-code)
}
med0 <- donors %>% summarise(m0=median(lr),.by=NAPR)
links <- links %>% left_join(med0,by='NAPR') %>% mutate(
  donor_level=case_when(n6>=3~'6',n4>=3~'4',n2>=3~'2',is.finite(m0)~'direction',TRUE~NA_character_),
  lr_med=case_when(n6>=3~m6,n4>=3~m4,n2>=3~m2,TRUE~m0),
  outlier=is.finite(lr)&is.finite(lr_med)&abs(lr-lr_med)>log(3),
  own_lr=if_else(outlier,NA_real_,lr),
  imputed=!is.finite(own_lr),
  r=exp(coalesce(own_lr,lr_med)),
  included=eligible&is.finite(r)&r>0,
  exclusion=case_when(!is.finite(v)|!is.finite(v0)~'incomplete_value_window',
    source_count!=1~'source_changed',!(v>0|v0>0)~'observed_zero_both_windows',
    !is.finite(r)|r<=0~'no_price_relative',TRUE~NA_character_))
arrow::write_parquet(links,file.path(args[2],'series.parquet'))
results <- list()
for(country in c('CN','ALL')) for(code in c('0','27')) for(direction in c('\u0418\u041c','\u042d\u041a')) {
  part <- links %>% filter(NAPR==direction,country=='ALL'|STRANA==country,code=='0'|substr(TNVED,1,2)==code)
  basket <- part %>% filter(included)
  A <- sum(basket$v0); B <- sum(basket$v); C <- sum(basket$v0*basket$r); D <- sum(basket$v/basket$r)
  q <- if(A>0&B>0&C>0&D>0) sqrt(D/A*B/C) else NA_real_
  p <- if(A>0&B>0&C>0&D>0) sqrt(C/A*B/D) else NA_real_
  obs <- raw %>% filter(NAPR==direction,country=='ALL'|STRANA==country,code=='0'|substr(TNVED,1,2)==code,is.finite(STOIM),STOIM>=0)
  current_available <- sum(obs$STOIM[obs$PERIOD>=as.Date('2025-09-01')])
  base_available <- sum(obs$STOIM[obs$PERIOD<=as.Date('2025-12-01')])
  stopifnot(nrow(basket)>0,is.finite(q),is.finite(p),current_available>0,base_available>0,
    all(basket$observed==12),all(basket$observed0==12),all(basket$source_count==1),
    B<=current_available+.01,A<=base_available+.01,
    is.na(q)||abs(q*p-B/A)<1e-10*max(1,B/A))
  results[[length(results)+1]] <- tibble(STRANA=country,NAPR=direction,TNVED_G=code,
    current_from=as.Date('2025-09-01'),current_to=as.Date('2026-08-01'),
    base_from=as.Date('2025-01-01'),base_to=as.Date('2025-12-01'),
    series=nrow(basket),A=A,B=B,C=C,D=D,q_link=q,p_link=p,
    current_coverage=B/current_available,base_coverage=A/base_available,
    imp=sum((basket$v+basket$v0)[basket$imputed])/(A+B),
    current_imp=sum(basket$v[basket$imputed])/B,base_imp=sum(basket$v0[basket$imputed])/A,
    current_available=current_available,base_available=base_available,
    excluded_price=sum(part$eligible & !part$included),
    interpretation='matched_basket_link_not_full_trade_index')
}
result <- bind_rows(results)
arrow::write_parquet(result,file.path(args[2],'summary.parquet'))
# Export CSV through DuckDB to preserve Cyrillic on Windows native locales.
csv_con <- dbConnect(duckdb())
duckdb_register(csv_con,'result',result)
dbExecute(csv_con,'COPY result TO ? (HEADER, DELIMITER \',\')',params=list(file.path(args[2],'summary.csv')))
dbDisconnect(csv_con,shutdown=TRUE)
print(result %>% select(STRANA,NAPR,TNVED_G,series,q_link,p_link,current_coverage,base_coverage,imp),width=Inf)
cat('PASS: complete matched windows, positive price relatives, coverage, Fisher identity\n')
