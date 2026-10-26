# Local experimental Fisher build. Existing production files are never overwritten.
# Run from repository root: Rscript <this file> [input.duckdb] [new-output-directory]
source('src/fizob_fisher.R')
library(DBI)
library(duckdb)
args <- commandArgs(trailingOnly=TRUE)
database <- if(length(args)>=1) args[[1]] else 'db/unified_trade_data.duckdb'
out <- if(length(args)>=2) args[[2]] else 'tmp/fizob-fisher-v2'
if(dir.exists(out)) stop('Output directory already exists; choose a new directory')
if(!dir.create(out,recursive=TRUE)) stop('Could not reserve output directory')
zero_file <- file.path(out,'confirmed_absence.parquet')
python <- Sys.getenv('FIZOB_PYTHON',unset='python')
status <- system2(python,c('scripts/prepare_fisher_publication_zeros.py',shQuote(database),shQuote(zero_file)))
if(status!=0) stop('Publication registry validation failed')
con <- dbConnect(duckdb(),database,read_only=TRUE)
input <- dbGetQuery(con, "SELECT STRANA,NAPR,TNVED,PERIOD,EDIZM,STOIM,NETTO,KOL,TYPE,SOURCE,
 KOL_ESTIMATED,NETTO_ESTIMATED FROM unified_trade_data")
dbDisconnect(con,shutdown=TRUE)
zeros <- arrow::read_parquet(zero_file)
if(nrow(zeros)) input <- bind_rows(input,zeros)
cat('Input:',nrow(input),'rows. Calculating fact only.\n')
fo <- calculate_fisher(input)
validate_fisher(fo)
rm(input)
for(level in c(0,2,4,6)) {
  part <- fo %>% filter(.data$level==.env$level) %>% select(-level)
  suffix <- if(level==0) '' else as.character(level)
  names(part)[names(part)=='fizob'] <- paste0('fizob',suffix)
  names(part)[names(part)=='price'] <- paste0('price',suffix)
  names(part)[names(part)=='TNVED_G'] <- paste0('TNVED',if(level==0) '2' else suffix)
  arrow::write_parquet(part,file.path(out,if(level==0) 'fizob_total.parquet' else paste0('fizob_',level,'.parquet')))
}
cat('Saved experimental results:',out,'\n')
