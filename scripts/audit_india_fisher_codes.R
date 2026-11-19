# Local sensitivity audit. Never modifies serving data or assigns missing zeros.
# All countries remain in the price-relative pool; Fisher formulas are unchanged.
source('src/fizob_fisher.R')
args <- commandArgs(trailingOnly=TRUE)
database <- if(length(args)) args[[1]] else 'db/unified_trade_data.duckdb'
out <- if(length(args)>1) args[[2]] else 'tmp/india-fisher-code-audit'
if(dir.exists(out)) stop('Choose a new output directory')
dir.create(out,recursive=TRUE)
con <- DBI::dbConnect(duckdb::duckdb(),database,read_only=TRUE)
raw <- DBI::dbGetQuery(con,"SELECT STRANA,NAPR,TNVED,PERIOD,EDIZM,STOIM,NETTO,KOL,TYPE,SOURCE,
 KOL_ESTIMATED,NETTO_ESTIMATED FROM unified_trade_data
 WHERE TYPE='fact' AND SOURCE IS NOT NULL AND SOURCE!='nowcast' AND STRANA!='ALL'")
DBI::dbDisconnect(con,shutdown=TRUE)
raw$PERIOD <- as.Date(raw$PERIOD)
calendar <- seq.Date(min(raw$PERIOD),max(raw$PERIOD),by='month')
links <- bind_rows(lapply(split(raw,raw$STRANA),function(part) {
  message('Windows: ',part$STRANA[1])
  fisher_links(part,calendar,.8)
}))
history <- raw %>% filter(STRANA=='IN',startsWith(TNVED,'27')) %>%
  summarise(first_row=min(PERIOD),last_row=max(PERIOD),
    last_positive=if(any(STOIM>0,na.rm=TRUE)) max(PERIOD[which(STOIM>0)]) else as.Date(NA),
    .by=c(STRANA,NAPR,TNVED,SOURCE))
write.csv(history,file.path(out,'india-hs27-history.csv'),row.names=FALSE)
rm(raw)
calculate_links <- function(x) {
  sums <- aggregate_fisher(x,3,3,c(0L,2L,4L),c('0','27','2709'))
  result <- chain_fisher_many(sums)
  validate_fisher(result)
  result
}
baseline <- calculate_links(links)
arrow::write_parquet(baseline,file.path(out,'strict.parquet'))
# HYPOTHESIS ONLY: end applicability after last observed year 2023.
# Both the current rolling year and reference calendar year must be later.
# Last observation is NOT evidence of retirement; this is never a production rule.
remove <- with(links,STRANA=='IN' & NAPR=='ЭК' &
  TNVED %in% c('2709000000','2709200000') &
  PERIOD-months(11)>as.Date('2023-12-01') &
  floor_date(PERIOD,'year')-years(1)>as.Date('2023-12-01'))
stopifnot(!any(is.finite(links$lr[remove])),
          all(links$available[remove]==0),all(links$observed[remove]==0),
          all(links$observed0[remove]==0))
scenario <- calculate_links(links[!remove,])
arrow::write_parquet(scenario,file.path(out,'HYPOTHESIS_NOT_FOR_PUBLICATION.parquet'))
# Removing wholly unknown historical keys must not change observed turnover,
# the price pool, or any other country's result.
keys <- c('STRANA','NAPR','level','TNVED_G','PERIOD')
joined <- inner_join(baseline,scenario,by=keys,suffix=c('_before','_after'))
stopifnot(isTRUE(all.equal(joined$available_stoim_12_before,
                          joined$available_stoim_12_after,tolerance=1e-12)))
stopifnot(isTRUE(all.equal(baseline %>% filter(!STRANA %in% c('IN','ALL')),
                          scenario %>% filter(!STRANA %in% c('IN','ALL')))))
summary <- joined %>% filter(STRANA %in% c('IN','ALL'),PERIOD==max(PERIOD)) %>%
  select(all_of(keys),starts_with('coverage'),starts_with('base_coverage'),
         starts_with('fizob'),starts_with('status'),starts_with('imp'))
write.csv(summary,file.path(out,'comparison.csv'),row.names=FALSE)
jsonlite::write_json(list(hypothesis_only=TRUE,removed_link_rows=sum(remove),
  turnover_preserved=TRUE,other_countries_unchanged=TRUE,price_pool_unchanged=TRUE,
  summary=summary),file.path(out,'audit.json'),pretty=TRUE,auto_unbox=TRUE,na='null')
message('Audit complete: ',out)
