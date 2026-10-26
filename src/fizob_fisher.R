# Experimental Fisher calculation. No filesystem writes on source().
# A missing publication is unknown; only an observed zero is zero trade.
suppressPackageStartupMessages(library(tidyverse))
suppressPackageStartupMessages(library(slider))

single_fisher_label <- function(x) {
  labels <- unique(x[!is.na(x)])
  if(length(labels)==1L && labels!='__mixed__') labels else NA_character_
}

fisher_links_reference <- function(raw, calendar, cov_min) {
  keys <- c('STRANA','NAPR','TNVED')
  # Compute physical coverage BEFORE summing subrows with different units/flags.
  monthly <- raw %>% mutate(
    valid_v=is.finite(STOIM) & STOIM>=0,
    ok_k=valid_v & STOIM>0 & is.finite(KOL) & KOL>0 &
      !is.na(EDIZM) & !EDIZM %in% c('', '?', 'NA'),
    ok_n=valid_v & STOIM>0 & is.finite(NETTO) & NETTO>0) %>%
    summarise(
      value=if(all(valid_v)) sum(STOIM) else NA_real_,
      available=sum(STOIM[valid_v]),
      vk=sum(STOIM[ok_k]), k=sum(KOL[ok_k]),
      vn=sum(STOIM[ok_n]), n=sum(NETTO[ok_n]),
      unit=if(n_distinct(EDIZM[ok_k])>1L) '__mixed__' else single_fisher_label(EDIZM[ok_k]),
      source=if(n_distinct(SOURCE)>1L) '__mixed__' else single_fisher_label(SOURCE),
      estimated=sum(STOIM[valid_v & (coalesce(KOL_ESTIMATED,FALSE) | coalesce(NETTO_ESTIMATED,FALSE))]),
      observed=as.integer(all(valid_v)), .by=c(STRANA,NAPR,TNVED,PERIOD))
  grid <- raw %>% distinct(across(all_of(keys))) %>% cross_join(tibble(PERIOD=calendar))
  monthly <- grid %>% left_join(monthly,by=c(keys,'PERIOD')) %>%
    arrange(STRANA,NAPR,TNVED,PERIOD)
  monthly %>% mutate(
    v=slide_sum(value,before=11,complete=TRUE),
    across(c(available,vk,k,vn,n,estimated,observed),
           ~slide_sum(coalesce(.x,0),before=11,complete=TRUE)),
    across(c(unit,source),~slide_chr(.x,single_fisher_label,.before=11,.complete=TRUE)),
    .by=all_of(keys)) %>% filter(PERIOD>=calendar[12]) %>%
    mutate(ref=floor_date(PERIOD,'year')-months(1)) %>%
    mutate(across(c(v,vk,k,vn,n,observed,unit,source),~.x[match(ref,PERIOD)],.names='{.col}0'),
           .by=all_of(keys)) %>%
    mutate(source_ok=!is.na(source) & !is.na(source0) & source==source0,
      kol_ok=!is.na(unit) & !is.na(unit0) & unit==unit0,
      lr=case_when(
      !source_ok | is.na(v) | is.na(v0) | v<=0 | v0<=0 ~ NA_real_,
      kol_ok & vk>=cov_min*v & vk0>=cov_min*v0 & k>0 & k0>0 ~ log(vk/k)-log(vk0/k0),
      vn>=cov_min*v & vn0>=cov_min*v0 & n>0 & n0>0 ~ log(vn/n)-log(vn0/n0)),
      vk=if_else(!is.na(unit) & (is.na(v0) | v0==0 | kol_ok),vk,0)) %>%
    select(STRANA,NAPR,TNVED,PERIOD,v,v0,lr,available,observed,observed0,estimated,vk,vn)
}

source('src/fizob_fisher_windows.R')
source('src/fizob_fisher_aggregate.R')
source('src/fizob_fisher_chain.R')

calculate_fisher <- function(input,cov_min=.8,uv_bound=3,n_min=3,output_levels=c(0L,2L,4L,6L),output_codes=NULL) {
  required <- c('STRANA','NAPR','TNVED','PERIOD','EDIZM','STOIM','NETTO','KOL','TYPE','SOURCE')
  stopifnot(all(required %in% names(input)))
  for (flag in c('KOL_ESTIMATED','NETTO_ESTIMATED'))
    if (!flag %in% names(input)) input[[flag]] <- NA
  # Comtrade source estimates remain fact; project predictions never enter fact.
  raw <- input %>% filter(TYPE=='fact',!is.na(SOURCE),SOURCE!='nowcast',STRANA!='ALL') %>%
    mutate(PERIOD=as.Date(PERIOD))
  if (!nrow(raw)) stop('No factual observations')
  calendar <- seq.Date(min(raw$PERIOD),max(raw$PERIOD),by='month')
  if(length(calendar)<12) stop('At least 12 calendar months required')
  # Bound temporary grid memory: prepare windows one country at a time.
  links <- bind_rows(lapply(split(raw,raw$STRANA),function(part) {
    message('Fisher windows: ',part$STRANA[1],', ',nrow(part),' factual rows')
    fisher_links(part,calendar=calendar,cov_min=cov_min)
  }))
  rm(raw)
  message('Fisher: pooling price relatives and aggregates')
  sums <- aggregate_fisher(links,uv_bound,n_min,output_levels,output_codes)
  rm(links)
  message('Fisher: chaining ',nrow(sums),' aggregate observations')
  sums %>% filter(level %in% output_levels,is.null(output_codes) | TNVED_G %in% output_codes) %>%
    chain_fisher_many()
}

chain_fisher <- function(x) {
  count <- nrow(x)
  x$fizob <- x$price <- rep(NA_real_,count)
  x$bp <- as.Date(rep(NA_character_,count))
  anchor_q <- anchor_p <- NA_real_; anchor_year <- NA_integer_; base <- as.Date(NA)
  for(i in seq_len(count)) {
    y <- year(x$PERIOD[i]); dec <- month(x$PERIOD[i])==12
    a<-x$A[i]; b<-x$B[i]; c<-x$C[i]; d<-x$D[i]
    valid <- all(is.finite(c(a,b,c,d))) && a>0 && b>0 && c>0 && d>0
    if(valid && !is.na(anchor_year) && anchor_year==y-1L) {
      x$fizob[i]<-anchor_q*sqrt(d/a*b/c); x$price[i]<-anchor_p*sqrt(c/a*b/d); x$bp[i]<-base
    } else if(dec && is.finite(b) && b>0) {
      # An unavailable December breaks the chain. A new complete year gets a new base.
      x$fizob[i]<-1; x$price[i]<-1; base<-as.Date(sprintf('%d-01-01',y)); x$bp[i]<-base
    } else if(is.finite(b) && b==0 && !is.na(base)) {
      x$fizob[i]<-0; x$bp[i]<-base
    }
    if(dec) { anchor_q<-x$fizob[i]; anchor_p<-x$price[i]; anchor_year<-if(is.finite(anchor_q)&&is.finite(anchor_p)) y else NA_integer_ }
  }
  x %>% mutate(stoim_12=B,available_stoim_12=available,
    coverage=observed/expected,base_coverage=observed0/expected,
    quantity_coverage=if_else(available>0,vk/available,NA_real_),
    mass_coverage=if_else(available>0,vn/available,NA_real_),
    source_estimated_value=estimated,
    imp=if_else(is.finite(A+B) & A+B>0,I/(A+B),NA_real_),
    status=case_when(coverage<1~'incomplete_window',is.na(fizob)~'unavailable_link',
                     PERIOD==bp+months(11)~'base',TRUE~'complete'),
    TYPE='fact',methodology_version='fisher_annual_v2') %>%
    select(PERIOD,fizob,price,bp,stoim_12,available_stoim_12,imp,coverage,base_coverage,
           quantity_coverage,mass_coverage,source_estimated_value,status,TYPE,methodology_version)
}

validate_fisher <- function(result) {
  stopifnot(!any(is.infinite(result$fizob)),!any(is.infinite(result$price)),
            all(result$coverage>=0 & result$coverage<=1),
            all(result$base_coverage>=0 & result$base_coverage<=1),
            all(is.na(result$fizob[result$coverage<1])))
  published <- is.finite(result$fizob)
  stopifnot(all(!is.na(result$bp[published])),all(result$fizob[published]>=0),
            all(is.finite(result$price[published & result$fizob>0])))
  check <- result %>% filter(!is.na(bp)) %>% arrange(PERIOD) %>%
    mutate(base_value=stoim_12[match(first(bp)+months(11),PERIOD)],
           expected=stoim_12/base_value,
           error=abs(fizob*price-expected),
           .by=c(STRANA,NAPR,level,TNVED_G,bp))
  stopifnot(all(is.finite(check$base_value[is.finite(check$fizob)])),
            all(check$base_value[is.finite(check$fizob)]>0))
  stopifnot(!any(check$error>1e-9*pmax(1,abs(check$expected)),na.rm=TRUE))
  invisible(TRUE)
}
