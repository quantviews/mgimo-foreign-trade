WITH
  base_data AS (
    SELECT
      TNVED,
      NAPR,
      DATE_TRUNC('month', PERIOD) AS PERIOD_month,
      PERIOD,
      STOIM,
      STRANA,
      KOL,
      NETTO,
      EDIZM
    FROM unified_trade_data.main.unified_trade_data
  ),
  edizm_table AS (
    SELECT
      TNVED,
      NAPR,
      string_agg(DISTINCT EDIZM, ', ') AS unique_edizms,
      COUNT(DISTINCT EDIZM) AS n_edizm
    FROM base_data
    GROUP BY TNVED, NAPR
  ),
  use_netto AS (
    SELECT TNVED, NAPR
    FROM edizm_table
    WHERE unique_edizms = '?' OR n_edizm = 2
  ),
  use_kol AS (
    SELECT TNVED, NAPR
    FROM edizm_table
    WHERE n_edizm = 1 AND unique_edizms <> '?'
  ),
  group_bounds AS (
    SELECT
      TNVED,
      NAPR,
      MIN(PERIOD_month) AS min_period,
      MAX(PERIOD_month) AS max_period
    FROM base_data
    GROUP BY TNVED, NAPR
  ),
  months AS (
    SELECT
      b.TNVED,
      b.NAPR,
      gs.PERIOD AS PERIOD
    FROM group_bounds AS b,
         LATERAL generate_series(b.min_period, b.max_period, interval '1 month') gs(PERIOD)
  ),
  filled AS (
    SELECT
      m.TNVED,
      m.NAPR,
      m.PERIOD,
      COALESCE(d.STOIM, 0) AS STOIM,
      COALESCE(d.STRANA, 'TUR') AS STRANA,
      COALESCE(d.KOL, 0) AS KOL,
      COALESCE(d.NETTO, 0) AS NETTO,
      COALESCE(d.EDIZM, '?') AS EDIZM
    FROM months AS m
    LEFT JOIN base_data AS d ON d.TNVED = m.TNVED
                             AND d.NAPR = m.NAPR
                             AND d.PERIOD_month = m.PERIOD
  ),
  fo_constr AS (
    SELECT TNVED, NAPR, 'netto' AS fo_constr FROM use_netto
    UNION ALL
    SELECT TNVED, NAPR, 'kol' AS fo_constr FROM use_kol
  ),
  joined AS (
    SELECT
      f.TNVED, f.NAPR, f.PERIOD, f.KOL, f.NETTO, f.STRANA, f.STOIM, f.EDIZM, c.fo_constr
    FROM filled AS f
    LEFT JOIN fo_constr AS c ON f.TNVED = c.TNVED AND f.NAPR = c.NAPR
  ),
  slide AS (
    SELECT
      j.TNVED, j.NAPR, j.PERIOD, j.STRANA, j.KOL, j.NETTO, j.fo_constr,
      AVG(j.KOL) OVER (PARTITION BY j.TNVED, j.NAPR ORDER BY j.PERIOD ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS fo_kol_6m,
      AVG(j.NETTO) OVER (PARTITION BY j.TNVED, j.NAPR ORDER BY j.PERIOD ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS fo_netto_6m
    FROM joined AS j
  ),
  base_values AS (
    SELECT
      s.TNVED,
      s.NAPR,
      MAX(CASE WHEN s.PERIOD = DATE '2019-01-01' THEN s.KOL END) AS base_KOL_candidate,
      MAX(CASE WHEN s.PERIOD = DATE '2019-01-01' THEN s.NETTO END) AS base_NETTO_candidate
    FROM slide AS s
    GROUP BY s.TNVED, s.NAPR
  ),
  avg_values AS (
    SELECT
      TNVED,
      NAPR,
      AVG(KOL) AS avg_KOL,
      AVG(NETTO) AS avg_NETTO
    FROM slide
    GROUP BY TNVED, NAPR
  ),
  final_base AS (
    SELECT
      b.TNVED,
      b.NAPR,
      CASE
        WHEN b.base_KOL_candidate IS NULL OR b.base_KOL_candidate = 0 THEN a.avg_KOL
        ELSE b.base_KOL_candidate
      END AS base_KOL,
      CASE
        WHEN b.base_NETTO_candidate IS NULL OR b.base_NETTO_candidate = 0 THEN a.avg_NETTO
        ELSE b.base_NETTO_candidate
      END AS base_NETTO
    FROM base_values b
    JOIN avg_values a ON b.TNVED = a.TNVED AND b.NAPR = a.NAPR
  ),
  result AS (
    SELECT
      f.TNVED, f.NAPR, f.PERIOD, f.STRANA, f.KOL, f.NETTO,
      f.fo_kol_6m, f.fo_netto_6m,
      CASE
        WHEN f.fo_constr = 'kol' THEN NULLIF(f.KOL, 0) / NULLIF(b.base_KOL, 0)
        ELSE NULLIF(f.NETTO, 0) / NULLIF(b.base_NETTO, 0)
      END AS FIZOB,
      f.fo_constr
    FROM slide f
    JOIN final_base b ON f.TNVED = b.TNVED AND f.NAPR = b.NAPR
  )
SELECT * FROM result
ORDER BY NAPR, TNVED, PERIOD;
