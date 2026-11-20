# Integrated local audit; no writes to serving data and no missing-to-zero rule.
source('src/fizob_fisher.R')
args <- commandArgs(trailingOnly=TRUE)
stopifnot(length(args)==4L,!dir.exists(args[[2]]))
database <- args[[1]]; out <- args[[2]]
oil_review <- read.csv(args[[3]],stringsAsFactors=FALSE)
patch <- jsonlite::read_json(args[[4]],simplifyVector=FALSE)
for (item in patch$evidence) {
  stopifnot(digest::digest(readBin(item$path,'raw',n=file.info(item$path)$size),
                        serialize=FALSE,algo='sha256')==item$sha256)
}
dir.create(out,recursive=TRUE)
con <- DBI::dbConnect(duckdb::duckdb(),database,read_only=TRUE)
raw <- DBI::dbGetQuery(con,"SELECT STRANA,NAPR,TNVED,PERIOD,EDIZM,STOIM,NETTO,KOL,TYPE,SOURCE,
 KOL_ESTIMATED,NETTO_ESTIMATED FROM unified_trade_data
 WHERE TYPE='fact' AND SOURCE IS NOT NULL AND SOURCE!='nowcast' AND STRANA!='ALL'")
DBI::dbDisconnect(con,shutdown=TRUE)
raw$PERIOD <- as.Date(raw$PERIOD)
calendar <- seq.Date(min(raw$PERIOD),max(raw$PERIOD),by='month')
latest <- max(calendar)
codes <- c('0','27','2709','29','2924','2933','292419','293349',
           '10','1006','100630','2926','292690','86','8608','860800')
levels <- c(0L,2L,4L,6L)
india <- filter(raw,STRANA=='IN')
changed <- india
for (p in patch$mass_adjustments) {
  ids <- which(changed$SOURCE=='national' & changed$NAPR==p$napr & changed$TNVED==p$tnved & changed$PERIOD==as.Date(p$period))
  stopifnot(length(ids)==1L,is.na(changed$NETTO[ids]),
            abs(changed$STOIM[ids]-p$expected_cost)<.01,p$value>0)
  changed$NETTO[ids] <- p$value
}
stopifnot(!any(changed$NAPR==patch$napr & changed$TNVED==patch$tnved & changed$PERIOD==as.Date(patch$period)))
changed <- bind_rows(changed,tibble(STRANA='IN',NAPR=patch$napr,TNVED=patch$tnved,
 PERIOD=as.Date(patch$period),EDIZM=NA_character_,STOIM=patch$value_usd,
 NETTO=patch$mass_kg,KOL=NA_real_,TYPE='fact',SOURCE='national',KOL_ESTIMATED=NA,NETTO_ESTIMATED=NA))
changed$ORIGINAL_TNVED <- changed$TNVED
source('src/code_correspondence.R')
registry <- load_local_code_families()
families <- registry$families
jsonlite::write_json(registry,file.path(out,'code-registry.json'),pretty=TRUE,auto_unbox=TRUE)
for (parent in names(families)) {
  children <- families[[parent]]
  scope <- changed$NAPR=='ИМ' & changed$SOURCE=='national' & changed$TNVED %in% c(parent,children)
  part <- changed[scope,]
  stopifnot(all(children %in% part$TNVED))
  transition <- min(part$PERIOD[part$TNVED %in% children])
  stopifnot(transition==as.Date(registry$rules[[parent]]$first_observed_successor))
  for (period in seq.Date(min(part$PERIOD),latest,by='month')) {
    expected <- if(period<transition) parent else children
    month <- part[part$PERIOD==period,]
    stopifnot(all(expected %in% month$TNVED),!anyDuplicated(month$TNVED),
              all(is.finite(month$STOIM)),all(month$STOIM>=0))
    if(period>=transition) {
      old <- month[month$TNVED==parent,]
      stopifnot(all(old$STOIM==0),!any(old$NETTO>0,na.rm=TRUE),!any(old$KOL>0,na.rm=TRUE))
    }
  }
  changed$TNVED[scope] <- parent
}
oil_scope <- changed$NAPR=='ЭК' & changed$SOURCE=='national' & startsWith(changed$TNVED,'2709')
oil <- changed[oil_scope,]
oil_months <- oil %>% summarise(local_usd=sum(STOIM),
    unknown_mass=sum(STOIM>0 & (!is.finite(NETTO) | NETTO<=0)),.by=PERIOD)
stopifnot(nrow(oil_months)==length(calendar),!anyDuplicated(oil_review$period))
for (i in seq_len(nrow(oil_months))) {
  record <- oil_review[oil_review$period==as.character(oil_months$PERIOD[i]),]
  stopifnot(nrow(record)==1L,oil_months$unknown_mass[i]==0,
    abs(oil_months$local_usd[i]-record$local_usd)<.01,
    record$status=='verified_exact_cumulative' ||
      (record$status=='verified_monthly' && abs(record$difference_usd)<.01))
}
# This is a source-reconciled HS4 observation, NOT a claim of zero for absent HS8.
changed$TNVED[oil_scope] <- '2709000000'
stopifnot(abs(sum(changed$STOIM,na.rm=TRUE)-sum(india$STOIM,na.rm=TRUE)-patch$value_usd)<.01)
arrow::write_parquet(changed,file.path(out,'india-analysis-input-NOT-FOR-SERVING.parquet'))

links <- bind_rows(lapply(split(raw,raw$STRANA),function(part) {
  message('Windows: ',part$STRANA[1]);fisher_links(part,calendar,.8)
}))
calc <- function(x) {
  result <- aggregate_fisher(x,3,3,levels,codes) %>% chain_fisher_many()
  validate_fisher(result); result
}
before <- calc(links)
links_after <- bind_rows(filter(links,STRANA!='IN'),fisher_links(changed,calendar,.8))
after <- calc(links_after)
arrow::write_parquet(before,file.path(out,'before.parquet'))
arrow::write_parquet(after,file.path(out,'after.parquet'))

# Reporting and independent validation can be rerun without recalculating Fisher.
python <- Sys.getenv('FIZOB_PYTHON','python')
status <- system2(python,c('scripts/validate_india_joint_audit.py',
 '--db',shQuote(database),'--out',shQuote(out),
 '--evidence',shQuote(args[[4]]),'--oil',shQuote(args[[3]])))
stopifnot(status==0L)
message('Joint audit complete: ',out)
