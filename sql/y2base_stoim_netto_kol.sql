WITH base_data AS (
  SELECT *, DATE_TRUNC('month', PERIOD) AS month_period
  FROM unified_trade_data
  WHERE PERIOD >= DATE '2019-01-01'
    AND STRANA = 'TR'
),

period_agg AS (
  SELECT NAPR, TNVED, STRANA, month_period,
    SUM(STOIM) AS sum_STOIM,
    SUM(NETTO) AS sum_NETTO,
    SUM(KOL) AS sum_KOL
  FROM base_data
  GROUP BY NAPR, TNVED, STRANA, month_period
),

base_period_year AS (
  SELECT NAPR, TNVED, STRANA,
    SUM(sum_STOIM) AS STOIM_base_year,
    SUM(sum_NETTO) AS NETTO_base_year,
    SUM(sum_KOL) AS KOL_base_year
  FROM period_agg
  WHERE month_period BETWEEN DATE '2019-01-01' AND DATE '2019-12-31'
  GROUP BY NAPR, TNVED, STRANA
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
    AND b.month_period BETWEEN DATE_TRUNC('month', a.month_period) - INTERVAL '11 months' 
                           AND a.month_period
  GROUP BY a.NAPR, a.TNVED, a.STRANA, a.month_period
),

-- ★ ЗАКОММЕНТИРОВАНО: stability CTE для проверки
/*
stability AS (
  SELECT a.NAPR, a.TNVED, a.STRANA, a.month_period,
    MIN(b.EDIZM) AS min_EDIZM,
    MAX(b.EDIZM) AS max_EDIZM,
    MIN(b.EDIZM_ISO) AS min_EDIZM_ISO,
    MAX(b.EDIZM_ISO) AS max_EDIZM_ISO
  FROM base_data a
  JOIN base_data b ON a.NAPR = b.NAPR 
    AND a.TNVED = b.TNVED 
    AND a.STRANA = b.STRANA
    AND (b.PERIOD BETWEEN DATE '2019-01-01' AND DATE '2019-12-31' 
         OR b.PERIOD BETWEEN DATE_TRUNC('month', a.PERIOD) - INTERVAL '11 months' 
                         AND DATE_TRUNC('month', a.PERIOD))
  GROUP BY a.NAPR, a.TNVED, a.STRANA, a.month_period
)
*/

SELECT
  a.NAPR, a.PERIOD, a.STRANA, a.TNVED, a.EDIZM, a.EDIZM_ISO,
  a.STOIM, a.NETTO, a.KOL,
  
  CASE WHEN p.STOIM_base_year > 0 
       THEN (l.STOIM_last_12m - p.STOIM_base_year) / p.STOIM_base_year 
       ELSE NULL 
  END AS STOIM_Y2base,
  
  CASE WHEN p.NETTO_base_year > 0 
       THEN (l.NETTO_last_12m - p.NETTO_base_year) / p.NETTO_base_year 
       ELSE NULL 
  END AS NETTO_Y2base,
  
  -- ★ ИСПРАВЛЕНО: убрана проверка стабильности, KOL_Y2base всегда считается
  CASE WHEN p.KOL_base_year > 0 
       THEN (l.KOL_last_12m - p.KOL_base_year) / p.KOL_base_year 
       ELSE NULL 
  END AS KOL_Y2base

FROM base_data a
LEFT JOIN last_12_months l ON a.NAPR = l.NAPR 
  AND a.TNVED = l.TNVED 
  AND a.STRANA = l.STRANA 
  AND DATE_TRUNC('month', a.PERIOD) = l.month_period

LEFT JOIN base_period_year p ON a.NAPR = p.NAPR 
  AND a.TNVED = p.TNVED 
  AND a.STRANA = p.STRANA

-- ★ ЗАКОММЕНТИРОВАНО: LEFT JOIN stability
/*
LEFT JOIN stability s ON a.NAPR = s.NAPR 
  AND a.TNVED = s.TNVED 
  AND a.STRANA = s.STRANA 
  AND DATE_TRUNC('month', a.PERIOD) = s.month_period
*/

WHERE a.PERIOD >= DATE '2021-01-01'
  AND a.EDIZM IS NOT NULL

ORDER BY a.NAPR, a.STRANA, a.TNVED, a.PERIOD;
