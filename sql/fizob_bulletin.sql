-- Shared bulletin / Superset series. Calculate before display-period filtering.
-- Match calendar months, not the twelfth preceding observation.
WITH total_raw AS (
    -- Undo country normalization before aggregating the country-total formula.
    -- This is not an average/sum of indices or of product-level aggregates.
    SELECT NAPR, PERIOD,
           CASE WHEN count(*) = count(fizob * fizob_bp)
                     AND bool_and(isfinite(fizob * fizob_bp))
                THEN sum(fizob * fizob_bp) END AS raw_fizob
    FROM fizob_enriched WHERE tn_level = 0 AND STRANA <> 'ALL'
    GROUP BY NAPR, PERIOD
), total_base AS (
    SELECT *, min(PERIOD) OVER (PARTITION BY NAPR) AS base_start
    FROM total_raw
), total_normalized AS (
    SELECT *, avg(raw_fizob) FILTER (
        WHERE PERIOD < base_start + INTERVAL '12 months'
    ) OVER (PARTITION BY NAPR) AS base_value
    FROM total_base
), series AS (
    SELECT STRANA, COUNTRY_NAME, NAPR, PERIOD, YEAR, tn_level, tn_code,
           fizob, TNVED4_NAME, TNVED2_NAME,
           json_extract_string(to_json(f), '$.base_period') AS chain_base,
           json_extract_string(to_json(f), '$.methodology_version') AS methodology
    FROM fizob_enriched f
    WHERE tn_level IN (0, 2, 4, 6)
    UNION ALL
    SELECT 'ALL', 'Все страны в базе', NAPR, PERIOD, year(PERIOD), 0, '0',
           raw_fizob / nullif(base_value, 0), NULL, NULL, NULL, NULL
    FROM total_normalized
    WHERE NOT EXISTS (
        SELECT 1 FROM fizob_enriched WHERE STRANA='ALL' AND tn_level=0
    )
    AND NOT EXISTS (
        SELECT 1 FROM fizob_enriched f
        WHERE json_extract_string(to_json(f), '$.methodology_version') IS NOT NULL
    )
)
SELECT c.STRANA,
       CASE c.STRANA WHEN 'CN' THEN 'Китай' WHEN 'IN' THEN 'Индия'
            WHEN 'TR' THEN 'Турция'
            WHEN 'ALL' THEN 'Все страны в базе'
            ELSE COALESCE(c.COUNTRY_NAME, c.STRANA) END AS country,
       c.NAPR,
       CASE c.NAPR WHEN 'ИМ' THEN 'Импорт' ELSE 'Экспорт' END AS direction,
       c.PERIOD, c.YEAR, c.tn_level, c.tn_code,
       CASE WHEN c.tn_level = 0 THEN 'Все товары'
            ELSE c.tn_code || ' · ' || COALESCE(r.TNVED_NAME, c.TNVED4_NAME, c.TNVED2_NAME, 'Название не указано')
       END AS product,
       c.fizob,
       CASE WHEN p.chain_base IS NOT DISTINCT FROM c.chain_base
                  AND p.methodology IS NOT DISTINCT FROM c.methodology
                  AND (c.methodology IS NULL OR c.chain_base IS NOT NULL)
            THEN p.fizob END AS fizob_previous_year,
       CASE WHEN p.fizob > 0 AND isfinite(p.fizob) AND isfinite(c.fizob)
                  AND p.chain_base IS NOT DISTINCT FROM c.chain_base
                  AND p.methodology IS NOT DISTINCT FROM c.methodology
                  AND (c.methodology IS NULL OR c.chain_base IS NOT NULL)
            THEN c.fizob / p.fizob - 1 END AS yoy,
       c.PERIOD >= (SELECT max(PERIOD) FROM series) - INTERVAL '35 months' AS recent_36_months
FROM series c
LEFT JOIN series p
  ON c.STRANA = p.STRANA AND c.NAPR = p.NAPR
 AND c.tn_level = p.tn_level AND c.tn_code = p.tn_code
 AND p.PERIOD = c.PERIOD - INTERVAL '1 year'
LEFT JOIN tnved_reference r
  ON r.TNVED_CODE = c.tn_code AND r.TNVED_LEVEL = c.tn_level
