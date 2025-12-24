WITH step1 AS (
    SELECT
        *,
        CASE WHEN fo_constr = 'netto' THEN netto_12 ELSE kol_12 END AS fo_unit,
        CASE WHEN fo_constr = 'netto' THEN NETTO_bp ELSE KOL_bp END AS fo_unit_bp
    FROM data_fo
),

step2 AS (
    SELECT
        STRANA,
        NAPR,
        TNVED2,
        PERIOD,
        SUM(fo_unit * share_TNVED2) AS fizob2,
        MIN(first_year_entry) AS bp
    FROM step1
    GROUP BY STRANA, NAPR, TNVED2, PERIOD
),

step3 AS (
    SELECT
        s2.*,
        (
            SELECT AVG(s22.fizob2)
            FROM step2 s22
            WHERE s22.STRANA = s2.STRANA
              AND s22.NAPR   = s2.NAPR
              AND s22.TNVED2 = s2.TNVED2
              AND s22.PERIOD BETWEEN s2.bp
                                   AND ADD_MONTHS(s2.bp, 11)
        ) AS fizob2_bp
    FROM step2 s2
)

SELECT
    STRANA,
    NAPR,
    TNVED2,
    PERIOD,
    fizob2 / fizob2_bp AS fizob2
FROM step3;