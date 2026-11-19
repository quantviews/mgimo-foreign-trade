# Local recalculation using only explicitly approved, provenance-bearing rows.
# The source database and production exports are never overwritten.
source('src/fizob_fisher.R')
args <- commandArgs(trailingOnly=TRUE)
out <- if(length(args)) args[[1]] else 'tmp/india-comtrade-20261119/fisher'
if(dir.exists(out)) stop('Choose a new output directory')
approval <- jsonlite::read_json('tmp/india-comtrade-20261119/decisions.json',simplifyVector=TRUE)
stopifnot(identical(approval$status,'review_complete'))
dir.create(out,recursive=TRUE)
con <- DBI::dbConnect(duckdb::duckdb(),approval$candidate_database,read_only=TRUE)
input <- DBI::dbGetQuery(con,"SELECT STRANA,NAPR,TNVED,PERIOD,EDIZM,STOIM,NETTO,KOL,TYPE,SOURCE,
  KOL_ESTIMATED,NETTO_ESTIMATED FROM unified_trade_data")
DBI::dbDisconnect(con,shutdown=TRUE)
if(approval$approved_row_count>0) {
  additions <- arrow::read_parquet('tmp/india-comtrade-20261119/approved-additions.parquet')
  stopifnot(nrow(additions)==approval$approved_row_count,
    all(additions$STRANA=='IN'),all(additions$TYPE=='fact'),all(additions$SOURCE=='comtrade'),
    all(additions$STOIM>0))
  key <- c('STRANA','NAPR','TNVED','PERIOD')
  stopifnot(nrow(inner_join(input %>% filter(TYPE=='fact'),additions,by=key))==0,
            !anyDuplicated(additions[key]))
  input <- bind_rows(input,additions)
}
# No publication-zero generator: unknown observations stay unknown.
fo <- calculate_fisher(input,output_levels=c(0L,2L,4L),output_codes=c('0','27','2709'))
validate_fisher(fo)
arrow::write_parquet(fo,file.path(out,'fisher.parquet'))
latest <- fo %>% filter(STRANA %in% c('IN','ALL'),PERIOD==max(PERIOD))
write.csv(latest,file.path(out,'latest.csv'),row.names=FALSE)
jsonlite::write_json(list(approved_rows=approval$approved_row_count,
  approved_mass_patches=approval$approved_mass_patch_count,
  calculated_rows=nrow(fo),zero_substitution=FALSE,validation_passed=TRUE,
  methodology='unchanged fisher_annual_v2'),file.path(out,'checks.json'),pretty=TRUE,auto_unbox=TRUE)
message('Local recalculation complete: ',out)
