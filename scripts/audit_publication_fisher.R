# Run after preparing fixture observations and zero supplement through publication_registry.
source('src/fizob_fisher.R')
args <- commandArgs(trailingOnly=TRUE)
x <- arrow::read_parquet(args[[1]])
z <- arrow::read_parquet(args[[2]])
stopifnot(nrow(z)==1,all(z$OBSERVATION_STATUS=='confirmed_absence'))
before <- calculate_fisher(x,output_levels=c(0L,2L),output_codes=c('0','27'))
after <- calculate_fisher(bind_rows(x,z),output_levels=c(0L,2L),output_codes=c('0','27'))
validate_fisher(before); validate_fisher(after)
b <- before %>% filter(STRANA=='ALL',PERIOD==as.Date('2020-12-01'))
a <- after %>% filter(STRANA=='ALL',PERIOD==as.Date('2020-12-01'))
stopifnot(nrow(a)==2,all(is.na(b$fizob)),all(b$coverage<1),
          all(a$coverage==1),all(is.finite(a$fizob)),
          all(a$available_stoim_12==b$available_stoim_12))
cat('PASS: verified absence closes coverage for 27 and ALL, without invented value or physical measurements\n')
