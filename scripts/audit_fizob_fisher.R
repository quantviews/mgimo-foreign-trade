# Local read-only audit of the experimental Fisher implementation.
# Executes its calculation expressions on fixtures; never runs its DB/export code.
suppressPackageStartupMessages(library(tidyverse))
suppressPackageStartupMessages(library(slider))
args <- commandArgs(trailingOnly = TRUE)
output <- if (length(args)) args[[1]] else 'tmp/fizob-fisher-audit.json'
source('src/fizob_fisher.R')
calculate <- function(input) calculate_fisher(input)
panel <- function(country,code,unit,value,quantity) {
  tibble(STRANA=country,NAPR='ЭК',TNVED=code,
         PERIOD=seq.Date(as.Date('2019-01-01'),as.Date('2021-12-01'),by='month'),
         EDIZM=unit,STOIM=value,NETTO=quantity,KOL=quantity,TYPE='fact',SOURCE='national') %>%
    mutate(across(where(is.character),~{Encoding(.x)<-'UTF-8'; .x}))
}
base <- bind_rows(panel('CN','2709000000','КИЛОГРАММ',100,10),
                  panel('IN','8703000000','ШТУКА',200,2)) %>%
  mutate(STOIM=if_else(year(PERIOD)>2019,STOIM*2,STOIM),
         NETTO=if_else(year(PERIOD)>2019,NETTO*2,NETTO),
         KOL=if_else(year(PERIOD)>2019,KOL*2,KOL))
baseline <- calculate(base)
validate_fisher(baseline)
no_base <- baseline %>% mutate(bp=as.Date(NA),fizob=999)
stopifnot(inherits(try(validate_fisher(no_base),silent=TRUE),'try-error'))
no_base_row <- baseline %>% filter(PERIOD!=as.Date('2019-12-01')) %>% mutate(fizob=999)
stopifnot(inherits(try(validate_fisher(no_base_row),silent=TRUE),'try-error'))
# DuckDB window implementation must match the readable R reference.
reference_input <- base %>% mutate(KOL_ESTIMATED=FALSE,NETTO_ESTIMATED=FALSE)
calendar <- seq.Date(min(base$PERIOD),max(base$PERIOD),by='month')
stopifnot(isTRUE(all.equal(as.data.frame(fisher_links(reference_input,calendar,.8)),
                         as.data.frame(fisher_links_reference(reference_input,calendar,.8)),
                         check.attributes=FALSE,tolerance=1e-10)))
total <- function(x,country='ALL') x %>% filter(STRANA==country,level==0,PERIOD==as.Date('2020-12-01')) %>%
  select(fizob,price,stoim_12,imp)
stopifnot(abs(total(baseline)$fizob-2)<1e-10,abs(total(baseline)$price-1)<1e-10)
all_rows <- baseline %>% filter(STRANA=='ALL',level==0)
country_rows <- baseline %>% filter(STRANA!='ALL',level==0) %>% summarise(expected=sum(stoim_12),.by=PERIOD)
stopifnot(max(abs(left_join(all_rows,country_rows,by='PERIOD')$stoim_12-country_rows$expected))<1e-8)
identity_error <- baseline %>% mutate(error=abs(fizob*price-stoim_12/first(stoim_12)),.by=c(STRANA,NAPR,level,TNVED_G,bp)) %>% summarise(error=max(error,na.rm=TRUE))
stopifnot(identity_error$error<1e-10)
# Different price and quantity movements; verify ALL from pooled A/B/C/D.
heterogeneous <- base %>% mutate(
  STOIM=case_when(year(PERIOD)==2019 ~ STOIM, STRANA=='CN' ~ 400, TRUE ~ 100),
  NETTO=case_when(year(PERIOD)==2019 ~ NETTO, STRANA=='CN' ~ 20, TRUE ~ 2),
  KOL=NETTO)
heterogeneous_result <- total(calculate(heterogeneous))
expected_q <- sqrt((400/300)*(500/300))
expected_p <- sqrt((300/300)*(500/400))
stopifnot(abs(heterogeneous_result$fizob-expected_q)<1e-10,
          abs(heterogeneous_result$price-expected_p)<1e-10)
rescaled <- heterogeneous %>% mutate(across(c(NETTO,KOL),~if_else(STRANA=='CN',.x*1000,.x)))
stopifnot(isTRUE(all.equal(total(calculate(rescaled)),heterogeneous_result)))
# A missing country year must differ from explicit zero trade.
missing <- base %>% filter(!(STRANA=='IN' & year(PERIOD)==2020))
zeros <- base %>% mutate(across(c(STOIM,NETTO,KOL),~if_else(STRANA=='IN' & year(PERIOD)==2020,0,.x)))
missing_result <- total(calculate(missing))
zero_result <- total(calculate(zeros))
stopifnot(is.na(missing_result$fizob), is.na(missing_result$stoim_12),
          is.finite(zero_result$fizob), zero_result$stoim_12==2400)
# An overlapping forecast must not change the factual observation.
forecast <- base %>% filter(STRANA=='CN',year(PERIOD)==2020) %>% mutate(TYPE='pred',SOURCE='nowcast')
contaminated <- total(calculate(bind_rows(base,forecast)))
stopifnot(isTRUE(all.equal(contaminated,total(baseline))))
# Unknown physical subrows cannot be credited as observed price coverage.
mixed <- base %>% filter(STRANA=='CN')
unknown <- mixed %>% mutate(STOIM=if_else(year(PERIOD)>2019,900,0),NETTO=NA_real_,KOL=NA_real_)
mixed_result <- total(calculate(bind_rows(mixed,unknown)),'CN')
stopifnot(is.na(mixed_result$price) || mixed_result$price != 5.5)
mixed_coverage <- calculate(bind_rows(mixed,unknown)) %>%
  filter(STRANA=='CN',level==0,PERIOD==as.Date('2020-12-01'))
stopifnot(abs(mixed_coverage$quantity_coverage-200/1100)<1e-10,
          abs(mixed_coverage$mass_coverage-200/1100)<1e-10)
# Preserve source estimates as fact, with an explicit quality marker.
source_estimates <- base %>% mutate(SOURCE='comtrade',KOL_ESTIMATED=TRUE,NETTO_ESTIMATED=FALSE)
estimated_result <- calculate(source_estimates) %>%
  filter(STRANA=='ALL',level==0,PERIOD==as.Date('2020-12-01'))
stopifnot(estimated_result$fizob==2, estimated_result$source_estimated_value==7200)
unknown_units <- base %>% mutate(EDIZM=NA_character_)
unknown_units_result <- calculate(unknown_units) %>%
  filter(STRANA=='ALL',level==0,PERIOD==as.Date('2020-12-01'))
stopifnot(unknown_units_result$quantity_coverage==0,
          unknown_units_result$mass_coverage==1, unknown_units_result$fizob==2)
# Missing December breaks the chain; rebase rather than silently joining it.
gap <- base %>% filter(!(STRANA=='CN' & PERIOD==as.Date('2020-12-01')))
gap_result <- calculate(gap) %>% filter(STRANA=='CN',level==0)
stopifnot(is.na(gap_result$fizob[gap_result$PERIOD==as.Date('2021-01-01')]),
          gap_result$bp[gap_result$PERIOD==as.Date('2021-12-01')]==as.Date('2021-01-01'))
# Future source/unit changes must not revise already calculated history.
future <- base %>% filter(year(PERIOD)==2021) %>%
  mutate(PERIOD=PERIOD+years(1),SOURCE='comtrade',EDIZM='NEW UNIT')
extended <- calculate(bind_rows(base,future)) %>% filter(PERIOD<=max(base$PERIOD))
stopifnot(isTRUE(all.equal(baseline,extended,check.attributes=FALSE)))
long_zero <- base %>% mutate(across(c(STOIM,NETTO,KOL),~if_else(STRANA=='IN' & year(PERIOD)>=2020,0,.x)))
zero_chain <- calculate(long_zero) %>% filter(STRANA=='IN',level==0,year(PERIOD)==2021)
stopifnot(all(zero_chain$fizob==0),all(zero_chain$coverage==1))
changed_units <- base %>% mutate(EDIZM=if_else(year(PERIOD)>=2020,'NEW UNIT',EDIZM),NETTO=NA_real_)
changed_result <- calculate(changed_units) %>% filter(level==0,PERIOD==as.Date('2020-12-01'))
stopifnot(all(changed_result$quantity_coverage==0))
for (fixture in list(missing,bind_rows(mixed,unknown),unknown_units,changed_units,bind_rows(base,future))) {
  fixture <- fixture %>% mutate(KOL_ESTIMATED=FALSE,NETTO_ESTIMATED=FALSE)
  fixture_calendar <- seq.Date(min(fixture$PERIOD),max(fixture$PERIOD),by='month')
  stopifnot(isTRUE(all.equal(as.data.frame(fisher_links(fixture,fixture_calendar,.8)),
    as.data.frame(fisher_links_reference(fixture,fixture_calendar,.8)),
    check.attributes=FALSE,tolerance=1e-10)))
}
report <- list(
  scope='Synthetic fixtures executed by the corrected Fisher functions; no production outputs written',
  baseline=total(baseline),identity_error=identity_error$error,
  all_value_equals_sum_of_countries=TRUE,
  heterogeneous_units_all=heterogeneous_result,
  pooled_fisher_expected=list(quantity=expected_q,price=expected_p),
  invariant_to_constant_unit_rescaling=TRUE,
  missing_year=missing_result,explicit_zero_year=zero_result,
  added_forecast_rows=contaminated,
  partial_physical_subrows=mixed_result,
  partial_physical_actual_value_coverage=200/1100)
dir.create(dirname(output),recursive=TRUE,showWarnings=FALSE)
jsonlite::write_json(report,output,pretty=TRUE,auto_unbox=TRUE,na='null')
cat('Audit saved:',output,'\n')
