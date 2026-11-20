# Scenario audit only: harmonize complete split families without editing raw data.
source('src/fizob_fisher.R')
args <- commandArgs(trailingOnly=TRUE)
stopifnot(length(args)==2L,!dir.exists(args[[2]]))
out <- args[[2]]
dir.create(out,recursive=TRUE)
con <- DBI::dbConnect(duckdb::duckdb(),args[[1]],read_only=TRUE)
raw <- DBI::dbGetQuery(con,"SELECT STRANA,NAPR,TNVED,PERIOD,EDIZM,STOIM,NETTO,KOL,TYPE,SOURCE,
 KOL_ESTIMATED,NETTO_ESTIMATED FROM unified_trade_data
 WHERE TYPE='fact' AND SOURCE IS NOT NULL AND SOURCE!='nowcast' AND STRANA!='ALL'")
DBI::dbDisconnect(con,shutdown=TRUE)
raw$PERIOD <- as.Date(raw$PERIOD)
calendar <- seq.Date(min(raw$PERIOD),max(raw$PERIOD),by='month')
source('src/code_correspondence.R')
registry <- load_local_code_families()
families <- registry$families
jsonlite::write_json(registry,file.path(out,'code-registry.json'),pretty=TRUE,auto_unbox=TRUE)
india <- filter(raw,STRANA=='IN')
changed <- india
audit <- list()
for (parent in names(families)) {
  children <- families[[parent]]
  scope <- india$NAPR=='ИМ' & india$SOURCE=='national' & india$TNVED %in% c(parent,children)
  part <- india[scope,]
  stopifnot(all(children %in% part$TNVED))
  transition <- min(part$PERIOD[part$TNVED %in% children])
  stopifnot(transition==as.Date(registry$rules[[parent]]$first_observed_successor))
  periods <- seq.Date(min(part$PERIOD),max(calendar),by='month')
  for (period in periods) {
    expected <- if(period<transition) parent else children
    month <- part[part$PERIOD==period,]
    stopifnot(all(expected %in% month$TNVED),!anyDuplicated(month$TNVED),
              all(is.finite(month$STOIM)),all(month$STOIM>=0))
    if(period>=transition) {
      old <- month[month$TNVED==parent,]
      # Prevent double counting an overlapping positive old representation.
      stopifnot(all(old$STOIM==0),!any(old$NETTO>0,na.rm=TRUE),!any(old$KOL>0,na.rm=TRUE))
    }
  }
  # The historical code is an internal family key only, not a new source label.
  changed$TNVED[scope] <- parent
  audit[[parent]] <- list(children=children,observed_transition=as.character(transition),
    legal_effective_date=registry$rules[[parent]]$legal_effective_date,
    transition_date_conflict=transition!=as.Date(registry$rules[[parent]]$legal_effective_date),
    all_expected_monthly_members_present=TRUE,rows=nrow(part),
    recent_12_months_usd=sum(part$STOIM[part$PERIOD>=max(calendar)-months(11)]))
}
# No values, units or missingness are changed; only analytical family keys.
stopifnot(identical(select(india,-TNVED),select(changed,-TNVED)))
links <- bind_rows(lapply(split(raw,raw$STRANA),function(part) {
  message('Windows: ',part$STRANA[1]); fisher_links(part,calendar,.8)
}))
rm(raw)
calc <- function(x) {
  result <- aggregate_fisher(x,3,3,c(0L,2L,4L,6L),c('0','27','2709','292419','293349')) %>% chain_fisher_many()
  validate_fisher(result);result
}
before <- calc(links)
after <- calc(bind_rows(filter(links,STRANA!='IN'),fisher_links(changed,calendar,.8)))
stopifnot(isTRUE(all.equal(filter(before,TNVED_G %in% c('27','2709')),
                         filter(after,TNVED_G %in% c('27','2709')))))
arrow::write_parquet(before,file.path(out,'before.parquet'))
arrow::write_parquet(after,file.path(out,'after.parquet'))
keys <- c('STRANA','NAPR','level','TNVED_G','PERIOD')
joined <- inner_join(before,after,by=keys,suffix=c('_before','_after'))
stopifnot(nrow(joined)==nrow(before),nrow(joined)==nrow(after),
          identical(is.na(joined$available_stoim_12_before),is.na(joined$available_stoim_12_after)),
          all(abs(joined$available_stoim_12_before-joined$available_stoim_12_after)<.01,na.rm=TRUE))
comparison <- joined %>%
  filter(STRANA %in% c('IN','ALL'),PERIOD==max(PERIOD)) %>%
  select(all_of(keys),starts_with('coverage'),starts_with('base_coverage'),
         starts_with('fizob'),starts_with('status'),starts_with('imp'))
write.csv(comparison,file.path(out,'comparison.csv'),row.names=FALSE)
jsonlite::write_json(list(scenario_only=TRUE,source_database_modified=FALSE,
 zero_imputation=FALSE,values_and_units_unchanged=TRUE,available_aggregate_values_unchanged=TRUE,fisher_formula_unchanged=TRUE,
 chapter27_unchanged=TRUE,families=audit,summary=comparison),file.path(out,'audit.json'),
 pretty=TRUE,auto_unbox=TRUE,na='null')
message('Completed family sensitivity audit: ',out)
