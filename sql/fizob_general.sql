WITH 
-- 0. Load base data
base AS (
    SELECT 
        STRANA,
        NAPR,
        TNVED,
        CAST(PERIOD AS DATE) AS PERIOD,
        EDIZM,
        STOIM,
        NETTO,
        KOL
    FROM unified_trade_data
),

-- 1. Keep only groups with any STOIM > 0
groups_with_positive AS (
    SELECT DISTINCT STRANA, NAPR, TNVED
    FROM base
    WHERE STOIM > 0
),

topped AS (
    SELECT b.*
    FROM base b
    JOIN groups_with_positive g USING (STRANA, NAPR, TNVED)
),

-- 3. Monthly ranges per group (fixed start = 2019-01-01)
ranges AS (
    SELECT
        STRANA,
        NAPR,
        TNVED,
        DATE '2019-01-01' AS period_min,
        (SELECT date_trunc('month', MAX(PERIOD)) FROM topped) AS period_max
    FROM topped
    GROUP BY STRANA, NAPR, TNVED
),

-- 4. Build monthly calendar (complete)
calendar AS (
    SELECT
        r.STRANA,
        r.NAPR,
        r.TNVED,
        gs AS PERIOD
    FROM ranges r,
    LATERAL (
        SELECT unnest(
            range(
                r.period_min,
                r.period_max + INTERVAL '1 month',
                INTERVAL '1 month'
            )
        ) AS gs
    )
),

-- 5. Join calendar; fill zeros; forward-fill EDIZM
completed AS (
    SELECT
        c.STRANA,
        c.NAPR,
        c.TNVED,
        c.PERIOD,
        COALESCE(t.STOIM, 0) AS STOIM,
        COALESCE(t.KOL,   0) AS KOL,
        COALESCE(t.NETTO, 0) AS NETTO,
        arg_max(
            CASE WHEN t.EDIZM IS NOT NULL THEN t.EDIZM END,
            CASE WHEN t.EDIZM IS NOT NULL THEN c.PERIOD END
        ) OVER (
            PARTITION BY c.STRANA, c.NAPR, c.TNVED
            ORDER BY c.PERIOD
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS EDIZM
    FROM calendar c
    LEFT JOIN topped t
        ON t.STRANA = c.STRANA
       AND t.NAPR  = c.NAPR
       AND t.TNVED = c.TNVED
       AND t.PERIOD = c.PERIOD
),

-- 6. Rolling 12m means
final_calc AS (
    SELECT
        *,
        AVG(KOL)   OVER (PARTITION BY STRANA, NAPR, TNVED ORDER BY PERIOD ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS kol_12,
        AVG(NETTO) OVER (PARTITION BY STRANA, NAPR, TNVED ORDER BY PERIOD ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS netto_12,
        AVG(STOIM) OVER (PARTITION BY STRANA, NAPR, TNVED ORDER BY PERIOD ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS stoim_12
    FROM completed
),

-- 7a. Compute first_year_entry
first_year AS (
    SELECT
        STRANA,
        TNVED,
        NAPR,
        date_trunc('year', MIN(PERIOD)) AS first_year_entry
    FROM final_calc
    WHERE STOIM > 0
    GROUP BY 1,2,3
),

-- 7b. Join back
joined AS (
    SELECT
        f.*,
        fy.first_year_entry,
        fy.first_year_entry + INTERVAL '11 months' AS last_entry
    FROM final_calc f
    LEFT JOIN first_year fy USING (STRANA, TNVED, NAPR)
),

-- 7c. Compute base-period averages
with_bp AS (
    SELECT
        *,
        AVG(CASE WHEN PERIOD BETWEEN first_year_entry AND last_entry THEN STOIM END)
            OVER (PARTITION BY STRANA, TNVED, NAPR) AS STOIM_bp,
        AVG(CASE WHEN PERIOD BETWEEN first_year_entry AND last_entry THEN KOL END)
            OVER (PARTITION BY STRANA, TNVED, NAPR) AS KOL_bp,
        AVG(CASE WHEN PERIOD BETWEEN first_year_entry AND last_entry THEN NETTO END)
            OVER (PARTITION BY STRANA, TNVED, NAPR) AS NETTO_bp
    FROM joined
),

-- 8. edizm_table
edizm_table AS (
    SELECT
        STRANA,
        TNVED,
        NAPR,
        STRING_AGG(DISTINCT EDIZM, ', ') AS unique_edizms,
        COUNT(DISTINCT EDIZM) AS n_edizm,
        CASE WHEN MAX(CASE WHEN NETTO > 0 AND KOL = 0 THEN 1 ELSE 0 END) = 1 THEN 1 ELSE 0 END AS use_netto,
        CASE
            WHEN STRING_AGG(DISTINCT EDIZM, ', ') IN ('?', 'NA', '') 
                 OR COUNT(DISTINCT EDIZM) > 1 
                 OR MAX(CASE WHEN NETTO > 0 AND KOL = 0 THEN 1 ELSE 0 END) = 1
            THEN 'netto'
            ELSE 'kol'
        END AS fo_constr
    FROM with_bp
    GROUP BY STRANA, TNVED, NAPR
),

-- 9. data_fo
data_fo AS (
    SELECT
        w.*,
        SUBSTR(TNVED,1,2) AS TNVED2,
        SUBSTR(TNVED,1,4) AS TNVED4,
        SUBSTR(TNVED,1,6) AS TNVED6,
        e.fo_constr
    FROM with_bp w
    LEFT JOIN edizm_table e USING (STRANA, TNVED, NAPR)
)

-- === FINAL SELECT ===
SELECT
    STRANA,
    NAPR,
    TNVED,
    PERIOD,
    EDIZM,
    STOIM,
    KOL,
    NETTO,
    kol_12,
    netto_12,
    stoim_12,
    STOIM_bp,
    KOL_bp,
    NETTO_bp,
    fo_constr,
    COALESCE(stoim_12 / NULLIF(SUM(stoim_12) OVER (PARTITION BY STRANA, NAPR, TNVED2, PERIOD), 0), 0) AS share_TNVED2,
    COALESCE(stoim_12 / NULLIF(SUM(stoim_12) OVER (PARTITION BY STRANA, NAPR, TNVED4, PERIOD), 0), 0) AS share_TNVED4,
    COALESCE(stoim_12 / NULLIF(SUM(stoim_12) OVER (PARTITION BY STRANA, NAPR, TNVED6, PERIOD), 0), 0) AS share_TNVED6,
    CASE 
        WHEN fo_constr = 'kol' THEN kol_12 / NULLIF(KOL_bp,0)
        ELSE netto_12 / NULLIF(NETTO_bp,0)
    END AS fizob
FROM data_fo
ORDER BY STRANA, NAPR, TNVED, PERIOD;