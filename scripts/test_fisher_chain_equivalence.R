source('src/fizob_fisher.R')
source('src/fizob_fisher_chain.R')
set.seed(6107)
x <- tidyr::expand_grid(STRANA=c('CN','TR','ALL'),NAPR=c('ИМ','ЭК'),
  level=2L,TNVED_G=sprintf('%02d',1:20),
  PERIOD=seq.Date(as.Date('2019-12-01'),as.Date('2026-09-01'),by='month'))
n<-nrow(x)
x$A<-runif(n,1,100); x$B<-runif(n,1,100)
x$C<-runif(n,1,100); x$D<-runif(n,1,100)
for(col in c('A','B','C','D')) {
  x[[col]][sample(n,n%/%8)]<-NA_real_
  x[[col]][sample(n,n%/%12)]<-0
}
x$available<-runif(n,0,100); x$vk<-x$vn<-x$available*.8
x$observed<-sample(0:12,n,TRUE); x$observed0<-sample(0:12,n,TRUE)
x$expected<-12; x$estimated<-0; x$I<-1
old<-x %>% group_by(STRANA,NAPR,level,TNVED_G) %>% group_modify(~chain_fisher(.x)) %>% ungroup()
new<-chain_fisher_many(x)
comparison<-all.equal(old,new,tolerance=1e-14)
print(comparison)
stopifnot(isTRUE(comparison))
cat('Chain equivalence within floating-point precision:',n,'rows, 120 series with gaps, zeroes and base resets\n')
