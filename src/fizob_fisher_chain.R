# Vectorized execution of chain_fisher: identical state machine, one pass per month.
chain_fisher_many <- function(x) {
  x <- x %>% arrange(STRANA,NAPR,level,TNVED_G,PERIOD)
  g <- x %>% group_by(STRANA,NAPR,level,TNVED_G) %>% group_indices()
  n <- max(g)
  aq <- ap <- rep(NA_real_,n); ay <- rep(NA_integer_,n)
  base <- rep(NA_real_,n)
  q <- p <- bp <- rep(NA_real_,nrow(x))
  for (ii in split(seq_len(nrow(x)),x$PERIOD)) {
    gg <- g[ii]; y <- lubridate::year(x$PERIOD[ii[1]])
    dec <- lubridate::month(x$PERIOD[ii[1]])==12
    a<-x$A[ii]; b<-x$B[ii]; c<-x$C[ii]; d<-x$D[ii]
    valid <- is.finite(a)&is.finite(b)&is.finite(c)&is.finite(d)&a>0&b>0&c>0&d>0
    linked <- valid & !is.na(ay[gg]) & ay[gg]==y-1L
    reset <- !linked & dec & is.finite(b) & b>0
    zero <- !linked & !reset & is.finite(b) & b==0 & !is.na(base[gg])
    j<-ii[linked]; h<-gg[linked]
    q[j]<-aq[h]*sqrt(d[linked]/a[linked]*b[linked]/c[linked])
    p[j]<-ap[h]*sqrt(c[linked]/a[linked]*b[linked]/d[linked]); bp[j]<-base[h]
    j<-ii[reset]; h<-gg[reset]
    q[j]<-p[j]<-1; base[h]<-as.numeric(as.Date(sprintf('%d-01-01',y))); bp[j]<-base[h]
    q[ii[zero]]<-0; bp[ii[zero]]<-base[gg[zero]]
    if(dec) {
      aq[gg]<-q[ii]; ap[gg]<-p[ii]
      ay[gg]<-ifelse(is.finite(q[ii])&is.finite(p[ii]),y,NA_integer_)
    }
  }
  x$fizob<-q; x$price<-p; x$bp<-as.Date(bp,origin='1970-01-01')
  x %>% mutate(stoim_12=B,available_stoim_12=available,
    coverage=observed/expected,base_coverage=observed0/expected,
    quantity_coverage=if_else(available>0,vk/available,NA_real_),
    mass_coverage=if_else(available>0,vn/available,NA_real_),
    source_estimated_value=estimated,
    imp=if_else(is.finite(A+B)&A+B>0,I/(A+B),NA_real_),
    status=case_when(coverage<1~'incomplete_window',is.na(fizob)~'unavailable_link',
      PERIOD==bp+months(11)~'base',TRUE~'complete'),
    TYPE='fact',methodology_version='fisher_annual_v2') %>%
    select(STRANA,NAPR,level,TNVED_G,PERIOD,fizob,price,bp,stoim_12,
      available_stoim_12,imp,coverage,base_coverage,quantity_coverage,
      mass_coverage,source_estimated_value,status,TYPE,methodology_version)
}
