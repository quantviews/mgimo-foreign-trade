# Local group 27 / ALL recalculation with a prevalidated publication supplement.
# Rscript scripts/validate_fisher_publications.R database zeros.parquet output.parquet [baseline.parquet]
source('src/fizob_fisher.R')
library(DBI); library(duckdb)
args <- commandArgs(trailingOnly=TRUE)
stopifnot(length(args)>=3,!file.exists(args[[3]]))
con <- dbConnect(duckdb(),args[[1]],read_only=TRUE)
x <- dbGetQuery(con,"SELECT STRANA,NAPR,TNVED,PERIOD,EDIZM,STOIM,NETTO,KOL,TYPE,SOURCE,KOL_ESTIMATED,NETTO_ESTIMATED FROM unified_trade_data")
dbDisconnect(con,shutdown=TRUE)
z <- arrow::read_parquet(args[[2]])
if(nrow(z)) x <- bind_rows(x,z)
r <- calculate_fisher(x,output_levels=c(0L,2L),output_codes=c('0','27'))
validate_fisher(r)
if(length(args)>=4 && nrow(z)==0) {
  previous <- arrow::read_parquet(args[[4]])
  stopifnot(isTRUE(all.equal(as.data.frame(r),as.data.frame(previous),tolerance=1e-9)))
  cat('PASS: no authorized zeros, no change from baseline\n')
}
arrow::write_parquet(r,args[[3]])
cat('PASS:',nrow(r),'validated rows;',nrow(z),'authorized zero rows\n')
print(r %>% filter(STRANA=='ALL',PERIOD==max(PERIOD)) %>%
        select(NAPR,TNVED_G,PERIOD,fizob,available_stoim_12,coverage,status))
