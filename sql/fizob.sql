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
    FROM
      unified_trade_data.main.unified_trade_data
  ),
  edizm_table AS (
    SELECT
      TNVED,
      NAPR,
      string_agg(DISTINCT EDIZM, ', ') AS unique_edizms,
      COUNT(DISTINCT EDIZM) AS n_edizm
    FROM
      base_data
    GROUP BY
      TNVED,
      NAPR
  ),
  use_netto AS (
    SELECT
      TNVED,
      NAPR
    FROM
      edizm_table
    WHERE
      unique_edizms = '?'
      OR n_edizm = 2
  ),
  use_kol AS (
    SELECT
      TNVED,
      NAPR
    FROM
      edizm_table
    WHERE
      n_edizm = 1
      AND unique_edizms <> '?'
  ),
  group_bounds AS (
    SELECT
      TNVED,
      NAPR,
      MIN(DATE_TRUNC('month', PERIOD)) AS min_period,
      MAX(DATE_TRUNC('month', PERIOD)) AS max_period
    FROM
      base_data
    GROUP BY
      TNVED,
      NAPR
  ),
  months AS (
    SELECT
      b.TNVED,
      b.NAPR,
      gs.PERIOD
    FROM
      group_bounds AS b,
      LATERAL generate_series(b.min_period, b.max_period, interval '1 month') gs (PERIOD)
  ),
  fo_constr AS (
    SELECT
      TNVED,
      NAPR,
      'netto' AS fo_constr
    FROM
      use_netto
    UNION ALL
    SELECT
      TNVED,
      NAPR,
      'kol' AS fo_constr
    FROM
      use_kol
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
      COALESCE(d.EDIZM, '?') AS EDIZM,
      c.fo_constr
    FROM
      months m
      LEFT JOIN base_data d ON d.TNVED = m.TNVED
      AND d.NAPR = m.NAPR
      AND DATE_TRUNC('month', d.PERIOD) = m.PERIOD
      LEFT JOIN fo_constr c ON m.TNVED = c.TNVED
      AND m.NAPR = c.NAPR
  ),
  slide AS (
    SELECT
      f.*,
      AVG(f.KOL) OVER (
        PARTITION BY
          f.TNVED,
          f.NAPR
        ORDER BY
          f.PERIOD ROWS BETWEEN 3 PRECEDING
          AND 3 FOLLOWING
      ) AS fo_kol_6m,
      AVG(f.NETTO) OVER (
        PARTITION BY
          f.TNVED,
          f.NAPR
        ORDER BY
          f.PERIOD ROWS BETWEEN 3 PRECEDING
          AND 3 FOLLOWING
      ) AS fo_netto_6m
    FROM
      filled f
  ),
  base_stats AS (
    SELECT
      s.TNVED,
      s.NAPR,
      COALESCE(
        MAX(
          CASE
            WHEN s.PERIOD = DATE '2019-01-01' THEN NULLIF(s.KOL, 0)
          END
        ),
        AVG(s.KOL)
      ) AS base_KOL,
      COALESCE(
        MAX(
          CASE
            WHEN s.PERIOD = DATE '2019-01-01' THEN NULLIF(s.NETTO, 0)
          END
        ),
        AVG(s.NETTO)
      ) AS base_NETTO
    FROM
      slide s
    GROUP BY
      s.TNVED,
      s.NAPR
  )
SELECT
  s.TNVED,
  s.NAPR,
  s.PERIOD,
  s.STRANA,
  s.KOL,
  s.NETTO,
  s.fo_kol_6m,
  s.fo_netto_6m,
  CASE
    WHEN s.fo_constr = 'kol' THEN NULLIF(s.KOL, 0) / NULLIF(b.base_KOL, 0)
    ELSE NULLIF(s.NETTO, 0) / NULLIF(b.base_NETTO, 0)
  END AS FIZOB,
  s.fo_constr
FROM
  slide s
  JOIN base_stats b ON s.TNVED = b.TNVED
  AND s.NAPR = b.NAPR
ORDER BY
  s.NAPR,
  s.TNVED,
  s.PERIOD;
