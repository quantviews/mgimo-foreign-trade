# Same monthly/window contract as fisher_links_reference(), executed in DuckDB.
# Avoid millions of R callbacks for rolling label consistency on the full panel.
fisher_links <- function(raw, calendar, cov_min) {
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con,shutdown=TRUE),add=TRUE)
  DBI::dbExecute(con,"SET threads=2")
  DBI::dbExecute(con,"SET memory_limit='2GB'")
  DBI::dbExecute(con,paste0('SET temp_directory=',DBI::dbQuoteString(con,file.path(tempdir(),'fisher-windows'))))
  duckdb::duckdb_register(con,'raw',raw)
  duckdb::duckdb_register(con,'calendar',data.frame(PERIOD=calendar))
  query <- "WITH marked AS (
    SELECT *,isfinite(STOIM) AND STOIM>=0 AS valid_v,
      isfinite(STOIM) AND STOIM>0 AND isfinite(KOL) AND KOL>0
        AND EDIZM IS NOT NULL AND EDIZM NOT IN ('','?','NA') AS ok_k,
      isfinite(STOIM) AND STOIM>0 AND isfinite(NETTO) AND NETTO>0 AS ok_n
    FROM raw
  ), monthly AS (
    SELECT STRANA,NAPR,TNVED,PERIOD,
      CASE WHEN bool_and(coalesce(valid_v,false)) THEN sum(STOIM) END AS value,
      coalesce(sum(STOIM) FILTER(WHERE valid_v),0) AS available,
      coalesce(sum(STOIM) FILTER(WHERE ok_k),0) AS vk,
      coalesce(sum(KOL) FILTER(WHERE ok_k),0) AS k,
      coalesce(sum(STOIM) FILTER(WHERE ok_n),0) AS vn,
      coalesce(sum(NETTO) FILTER(WHERE ok_n),0) AS n,
      CASE WHEN count(DISTINCT EDIZM) FILTER(WHERE ok_k)>1 THEN '__mixed__'
           ELSE max(EDIZM) FILTER(WHERE ok_k) END AS unit,
      CASE WHEN count(DISTINCT SOURCE)>1 THEN '__mixed__' ELSE max(SOURCE) END AS source,
      coalesce(sum(STOIM) FILTER(WHERE valid_v AND
        (coalesce(KOL_ESTIMATED,false) OR coalesce(NETTO_ESTIMATED,false))),0) AS estimated,
      bool_and(coalesce(valid_v,false))::INTEGER AS observed
    FROM marked GROUP BY STRANA,NAPR,TNVED,PERIOD
  ), panel AS (
    SELECT keys.*,calendar.PERIOD,m.* EXCLUDE(STRANA,NAPR,TNVED,PERIOD)
    FROM (SELECT DISTINCT STRANA,NAPR,TNVED FROM raw) keys CROSS JOIN calendar
    LEFT JOIN monthly m USING(STRANA,NAPR,TNVED,PERIOD)
  ), windows AS (
    SELECT STRANA,NAPR,TNVED,PERIOD,
      CASE WHEN count(value) OVER w=12 THEN sum(value) OVER w END AS v,
      sum(coalesce(available,0)) OVER w AS available,
      sum(coalesce(vk,0)) OVER w AS vk,sum(coalesce(k,0)) OVER w AS k,
      sum(coalesce(vn,0)) OVER w AS vn,sum(coalesce(n,0)) OVER w AS n,
      sum(coalesce(estimated,0)) OVER w AS estimated,
      sum(coalesce(observed,0)) OVER w AS observed,
      CASE WHEN min(unit) OVER w=max(unit) OVER w AND min(unit) OVER w!='__mixed__'
           THEN min(unit) OVER w END AS unit,
      CASE WHEN min(source) OVER w=max(source) OVER w AND min(source) OVER w!='__mixed__'
           THEN min(source) OVER w END AS source
    FROM panel WINDOW w AS (PARTITION BY STRANA,NAPR,TNVED ORDER BY PERIOD ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)
  ), complete_calendar AS (
    SELECT * FROM windows WHERE PERIOD>=(SELECT min(PERIOD)+INTERVAL '11 months' FROM calendar)
  ), pairs AS (
    SELECT a.*,b.v AS v0,b.vk AS vk0,b.k AS k0,b.vn AS vn0,b.n AS n0,b.observed AS observed0,
      coalesce(a.unit=b.unit,false) AS kol_ok,
      coalesce(a.source=b.source,false) AS source_ok
    FROM complete_calendar a LEFT JOIN complete_calendar b
      ON a.STRANA=b.STRANA AND a.NAPR=b.NAPR AND a.TNVED=b.TNVED
      AND b.PERIOD=date_trunc('year',a.PERIOD)-INTERVAL '1 month'
  )
  SELECT STRANA,NAPR,TNVED,PERIOD,v,v0,
    CASE WHEN NOT source_ok OR v IS NULL OR v0 IS NULL OR v<=0 OR v0<=0 THEN NULL
      WHEN kol_ok AND vk>=COV_MIN*v AND vk0>=COV_MIN*v0 AND k>0 AND k0>0 THEN ln(vk/k)-ln(vk0/k0)
      WHEN vn>=COV_MIN*v AND vn0>=COV_MIN*v0 AND n>0 AND n0>0 THEN ln(vn/n)-ln(vn0/n0)
    END AS lr,available,observed,observed0,estimated,
    CASE WHEN unit IS NOT NULL AND (v0 IS NULL OR v0=0 OR kol_ok) THEN vk ELSE 0 END AS vk,vn
  FROM pairs ORDER BY STRANA,NAPR,TNVED,PERIOD"
  DBI::dbGetQuery(con,gsub('COV_MIN',format(cov_min,scientific=FALSE),query,fixed=TRUE))
}
