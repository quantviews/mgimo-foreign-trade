WITH base_data AS (
  SELECT *, DATE_TRUNC('month', PERIOD) AS month_period
  FROM unified_trade_data
  WHERE PERIOD >= DATE '2019-01-01'
),
period_agg AS (
  SELECT NAPR, TNVED, STRANA, month_period,
    SUM(STOIM) AS sum_STOIM,
    SUM(NETTO) AS sum_NETTO,
    SUM(KOL) AS sum_KOL
  FROM base_data
  GROUP BY NAPR, TNVED, STRANA, month_period
),
last_12_months AS (
  SELECT a.NAPR, a.TNVED, a.STRANA, a.month_period,
    COALESCE(SUM(b.sum_STOIM), 0) AS STOIM_last_12m,
    COALESCE(SUM(b.sum_NETTO), 0) AS NETTO_last_12m,
    COALESCE(SUM(b.sum_KOL), 0) AS KOL_last_12m
  FROM period_agg a
  LEFT JOIN period_agg b ON a.NAPR = b.NAPR 
    AND a.TNVED = b.TNVED 
    AND a.STRANA = b.STRANA
    AND b.month_period BETWEEN DATE_TRUNC('month', a.month_period) - INTERVAL '11 months' AND a.month_period
  GROUP BY a.NAPR, a.TNVED, a.STRANA, a.month_period
),
prev_12_months AS (
  SELECT a.NAPR, a.TNVED, a.STRANA, a.month_period,
    COALESCE(SUM(b.sum_STOIM), 0) AS STOIM_prev_12m,
    COALESCE(SUM(b.sum_NETTO), 0) AS NETTO_prev_12m,
    COALESCE(SUM(b.sum_KOL), 0) AS KOL_prev_12m
  FROM period_agg a
  LEFT JOIN period_agg b ON a.NAPR = b.NAPR 
    AND a.TNVED = b.TNVED 
    AND a.STRANA = b.STRANA
    AND b.month_period BETWEEN DATE_TRUNC('month', a.month_period) - INTERVAL '23 months' AND DATE_TRUNC('month', a.month_period) - INTERVAL '12 months'
  GROUP BY a.NAPR, a.TNVED, a.STRANA, a.month_period
),
last_24_months_stability AS (
  SELECT a.NAPR, a.TNVED, a.STRANA, a.month_period,
    MIN(b.EDIZM) AS min_EDIZM,
    MAX(b.EDIZM) AS max_EDIZM,
    MIN(b.EDIZM_ISO) AS min_EDIZM_ISO,
    MAX(b.EDIZM_ISO) AS max_EDIZM_ISO
  FROM base_data a
  JOIN base_data b ON a.NAPR = b.NAPR 
    AND a.TNVED = b.TNVED 
    AND a.STRANA = b.STRANA
    AND b.PERIOD BETWEEN DATE_TRUNC('month', a.PERIOD) - INTERVAL '23 months' AND DATE_TRUNC('month', a.PERIOD)
  GROUP BY a.NAPR, a.TNVED, a.STRANA, a.month_period
)

SELECT
  a.NAPR,
  a.PERIOD,
  a.STRANA,
  a.TNVED,
  a.EDIZM,
  a.EDIZM_ISO,
  a.STOIM,
  a.NETTO,
  a.KOL,
  CASE 
    WHEN p.STOIM_prev_12m <> 0 
    THEN (l.STOIM_last_12m - p.STOIM_prev_12m) / p.STOIM_prev_12m 
    ELSE NULL 
  END AS STOIM_Y2Y,
  CASE 
    WHEN p.NETTO_prev_12m <> 0 
    THEN (l.NETTO_last_12m - p.NETTO_prev_12m) / p.NETTO_prev_12m 
    ELSE NULL 
  END AS NETTO_Y2Y,
  CASE 
    WHEN p.KOL_prev_12m <> 0 
      AND s.min_EDIZM = s.max_EDIZM 
      AND s.min_EDIZM_ISO = s.max_EDIZM_ISO 
    THEN (l.KOL_last_12m - p.KOL_prev_12m) / p.KOL_prev_12m 
    ELSE NULL 
  END AS KOL_Y2Y
FROM base_data a
LEFT JOIN last_12_months l ON a.NAPR = l.NAPR 
  AND a.TNVED = l.TNVED 
  AND a.STRANA = l.STRANA 
  AND DATE_TRUNC('month', a.PERIOD) = l.month_period
LEFT JOIN prev_12_months p ON a.NAPR = p.NAPR 
  AND a.TNVED = p.TNVED 
  AND a.STRANA = p.STRANA 
  AND DATE_TRUNC('month', a.PERIOD) = p.month_period
LEFT JOIN last_24_months_stability s ON a.NAPR = s.NAPR 
  AND a.TNVED = s.TNVED 
  AND a.STRANA = s.STRANA 
  AND DATE_TRUNC('month', a.PERIOD) = s.month_period
WHERE a.PERIOD >= DATE '2021-01-01'
ORDER BY a.NAPR, a.STRANA, a.TNVED, a.PERIOD;
