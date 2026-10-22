-- Input tables are local TEMP tables: bulletin (one level) and monthly_trade.
-- A source month is present only if it contains actual fact rows.
WITH windows AS (
  SELECT b.*,
    sum(m.value_usd) OVER w AS value_12m_usd,
    count(m.source_rows) OVER w AS months_with_records,
    max(CASE WHEN m.source_rows > 0 THEN b.PERIOD END)
      OVER (PARTITION BY b.STRANA,b.NAPR,b.tn_level,b.tn_code) AS last_source_month
  FROM bulletin b
  LEFT JOIN monthly_trade m USING(STRANA,NAPR,tn_code,PERIOD)
  WINDOW w AS (PARTITION BY b.STRANA,b.NAPR,b.tn_level,b.tn_code
    ORDER BY b.PERIOD RANGE BETWEEN INTERVAL '11 months' PRECEDING AND CURRENT ROW)
)
SELECT c.*,
  p.value_12m_usd AS previous_value_12m_usd,
  p.months_with_records AS previous_months_with_records,
  CAST(c.PERIOD - INTERVAL '11 months' AS DATE) AS window_from,
  CAST(c.PERIOD - INTERVAL '23 months' AS DATE) AS previous_window_from,
  CAST(c.PERIOD - INTERVAL '1 year' AS DATE) AS previous_window_to
FROM windows c LEFT JOIN windows p
  ON c.STRANA=p.STRANA AND c.NAPR=p.NAPR AND c.tn_level=p.tn_level
 AND c.tn_code=p.tn_code AND p.PERIOD=c.PERIOD - INTERVAL '1 year'
