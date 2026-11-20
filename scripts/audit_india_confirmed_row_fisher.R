# Read-only local before/after audit. Source and serving databases are untouched.
source('src/fizob_fisher.R')
args <- commandArgs(trailingOnly=TRUE)
stopifnot(length(args)==3L)
database <- args[[1]]; evidence <- args[[2]]; out <- args[[3]]
stopifnot(!dir.exists(out))
patch <- jsonlite::read_json(evidence,simplifyVector=FALSE)
stopifnot(patch$source=='national_meidb_verified',patch$value_usd>0,
          patch$mass_kg>0,patch$unit=='KGS')
for (item in patch$evidence) {
  body <- readBin(item$path,'raw',n=file.info(item$path)$size)
  stopifnot(digest::digest(body,serialize=FALSE,algo='sha256')==item$sha256)
}
dir.create(out,recursive=TRUE)
con <- DBI::dbConnect(duckdb::duckdb(),database,read_only=TRUE)
raw <- DBI::dbGetQuery(con,"SELECT STRANA,NAPR,TNVED,PERIOD,EDIZM,STOIM,NETTO,KOL,TYPE,SOURCE,
 KOL_ESTIMATED,NETTO_ESTIMATED FROM unified_trade_data
 WHERE TYPE='fact' AND SOURCE IS NOT NULL AND SOURCE!='nowcast' AND STRANA!='ALL'")
DBI::dbDisconnect(con,shutdown=TRUE)
raw$PERIOD <- as.Date(raw$PERIOD)
stopifnot(!any(raw$STRANA=='IN' & raw$NAPR==patch$napr & raw$TNVED==patch$tnved & raw$PERIOD==as.Date(patch$period)))
calendar <- seq.Date(min(raw$PERIOD),max(raw$PERIOD),by='month')
links <- bind_rows(lapply(split(raw,raw$STRANA),function(part) {
  message('Windows: ',part$STRANA[1])
  fisher_links(part,calendar,.8)
}))
india <- raw %>% filter(STRANA=='IN')
addition <- tibble(STRANA='IN',NAPR=patch$napr,TNVED=patch$tnved,
 PERIOD=as.Date(patch$period),EDIZM=NA_character_,STOIM=patch$value_usd,
 NETTO=patch$mass_kg,KOL=NA_real_,TYPE='fact',SOURCE='national',
 KOL_ESTIMATED=NA,NETTO_ESTIMATED=NA)
# KGS is normalized into NETTO; it is not an independent additional quantity.
india_after <- bind_rows(india,addition)
for (p in patch$mass_adjustments) {
  ids <- which(india_after$NAPR==p$napr & india_after$TNVED==p$tnved & india_after$PERIOD==as.Date(p$period))
  stopifnot(length(ids)==1L,is.na(india_after$NETTO[ids]),
            abs(india_after$STOIM[ids]-p$expected_cost)<.01,p$value>0)
  india_after$NETTO[ids] <- p$value
}
stopifnot(nrow(india_after)==nrow(india)+1L,
          sum(india_after$STOIM,na.rm=TRUE)-sum(india$STOIM,na.rm=TRUE)==patch$value_usd)
rm(raw)
calc <- function(x) {
  result <- aggregate_fisher(x,3,3,c(0L,2L,4L),c('0','27','2709')) %>% chain_fisher_many()
  validate_fisher(result)
  result
}
before <- calc(links)
links_after <- bind_rows(filter(links,STRANA!='IN'),fisher_links(india_after,calendar,.8))
after <- calc(links_after)
stopifnot(isTRUE(all.equal(filter(before,TNVED_G %in% c('27','2709')),
                         filter(after,TNVED_G %in% c('27','2709')))))
arrow::write_parquet(before,file.path(out,'before.parquet'))
arrow::write_parquet(after,file.path(out,'after.parquet'))
keys <- c('STRANA','NAPR','level','TNVED_G','PERIOD')
comparison <- inner_join(before,after,by=keys,suffix=c('_before','_after')) %>%
  filter(STRANA %in% c('IN','ALL'),PERIOD==max(PERIOD)) %>%
  select(all_of(keys),starts_with('coverage'),starts_with('base_coverage'),
         starts_with('fizob'),starts_with('status'),starts_with('imp'),starts_with('mass_coverage'))
write.csv(comparison,file.path(out,'comparison.csv'),row.names=FALSE)
jsonlite::write_json(list(added_rows=1L,added_usd=patch$value_usd,added_mass_kg=patch$mass_kg,
 filled_mass_fields=length(patch$mass_adjustments),
 database_modified=FALSE,zero_imputation=FALSE,methodology_changed=FALSE,
 summary=comparison),file.path(out,'audit.json'),pretty=TRUE,auto_unbox=TRUE,na='null')
message('Audit complete: ',out)
