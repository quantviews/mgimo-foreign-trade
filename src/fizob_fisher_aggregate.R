# Pool Fisher terms, not normalized country indices. DuckDB bounds RAM and spills
# temporary intermediate tables locally; no persistent working DB is modified.
aggregate_fisher <- function(links,uv_bound,n_min,output_levels,output_codes) {
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con,shutdown=TRUE),add=TRUE)
  DBI::dbExecute(con,"SET threads=2")
  DBI::dbExecute(con,"SET memory_limit='3GB'")
  DBI::dbExecute(con,paste0('SET temp_directory=',DBI::dbQuoteString(con,file.path(tempdir(),'fisher-aggregate'))))
  duckdb::duckdb_register(con,'links',links)
  for(level in c(6,4,2)) {
    DBI::dbExecute(con,sprintf("CREATE TEMP TABLE med%d AS
      SELECT NAPR,substr(TNVED,1,%d) code,PERIOD,median(lr) m,count(*) n
      FROM links WHERE isfinite(lr) GROUP BY NAPR,code,PERIOD",level,level))
  }
  DBI::dbExecute(con,"CREATE TEMP TABLE med0 AS
    SELECT NAPR,PERIOD,median(lr) m FROM links WHERE isfinite(lr) GROUP BY NAPR,PERIOD")
  query <- sprintf("CREATE TEMP TABLE weighted AS WITH centers AS (
    SELECT l.*,CASE WHEN m6.n>=%d THEN m6.m WHEN m4.n>=%d THEN m4.m
      WHEN m2.n>=%d THEN m2.m ELSE m0.m END AS lr_med
    FROM links l
    LEFT JOIN med6 m6 ON l.NAPR=m6.NAPR AND l.PERIOD=m6.PERIOD AND substr(l.TNVED,1,6)=m6.code
    LEFT JOIN med4 m4 ON l.NAPR=m4.NAPR AND l.PERIOD=m4.PERIOD AND substr(l.TNVED,1,4)=m4.code
    LEFT JOIN med2 m2 ON l.NAPR=m2.NAPR AND l.PERIOD=m2.PERIOD AND substr(l.TNVED,1,2)=m2.code
    LEFT JOIN med0 m0 ON l.NAPR=m0.NAPR AND l.PERIOD=m0.PERIOD
  ), cleaned AS (
    SELECT * EXCLUDE(lr),CASE WHEN abs(lr-lr_med)>ln(%f) THEN NULL ELSE lr END AS lr
    FROM centers
  ), prices AS (SELECT *,exp(coalesce(lr,lr_med)) r FROM cleaned)
  SELECT STRANA,NAPR,TNVED,PERIOD,v0 A,v B,
    CASE WHEN v0=0 THEN 0 ELSE v0*r END C,
    CASE WHEN v=0 THEN 0 ELSE v/r END D,
    CASE WHEN lr IS NULL THEN v0+v ELSE 0 END I,
    available,observed,coalesce(observed0,0) observed0,12 AS expected,estimated,vk,vn
  FROM prices",n_min,n_min,n_min,uv_bound)
  DBI::dbExecute(con,query)
  strict <- paste(sprintf('CASE WHEN count(%s)=count(*) THEN sum(%s) END AS %s',
                          c('A','B','C','D','I'),c('A','B','C','D','I'),c('A','B','C','D','I')),collapse=',')
  known <- c('available','observed','observed0','expected','estimated','vk','vn')
  totals <- paste(sprintf('sum(%s) AS %s',known,known),collapse=',')
  result <- lapply(output_levels,function(level) {
    code <- if(level==0) "'0'" else sprintf('substr(TNVED,1,%d)',level)
    where <- if(is.null(output_codes)) '' else paste0(' WHERE ',code,' IN (',
      paste(DBI::dbQuoteString(con,output_codes),collapse=','),')')
    DBI::dbGetQuery(con,sprintf("SELECT coalesce(STRANA,'ALL') AS STRANA,NAPR,
      %s AS TNVED_G,PERIOD,%d AS level,%s,%s FROM weighted%s
      GROUP BY GROUPING SETS ((STRANA,NAPR,TNVED_G,PERIOD),(NAPR,TNVED_G,PERIOD))
      ORDER BY STRANA,NAPR,TNVED_G,PERIOD",code,level,strict,totals,where))
  })
  bind_rows(result)
}
