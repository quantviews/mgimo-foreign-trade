-- Оптимизация DuckDB для Superset
-- Файл: /data/duckdb/unified_trade_data.duckdb
--
-- Запуск (на VPS):
--   docker cp db/optimize_duckdb.sql superset-superset-1:/tmp/optimize_duckdb.sql
--   docker exec superset-superset-1 python -c "import duckdb; c=duckdb.connect('/data/duckdb/unified_trade_data.duckdb'); c.execute(open('/tmp/optimize_duckdb.sql').read()); c.close()"
--
-- После прогона в Superset переключить датасеты:
--   trade_mom_kpi      → trade_mom_kpi_mat
--   coverage_matrix    → coverage_matrix_mat
--   (опционально) unified_trade_data_enriched → unified_trade_data_enriched_light

PRAGMA threads = 6;

-- =============================================================================
-- 1. Индексы на базовых таблицах
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_trade_period
    ON unified_trade_data (PERIOD);

CREATE INDEX IF NOT EXISTS idx_trade_napr_period
    ON unified_trade_data (NAPR, PERIOD);

CREATE INDEX IF NOT EXISTS idx_trade_strana_period
    ON unified_trade_data (STRANA, PERIOD);

CREATE INDEX IF NOT EXISTS idx_trade_source_napr_period
    ON unified_trade_data (SOURCE, NAPR, PERIOD);

CREATE INDEX IF NOT EXISTS idx_trade_tnved2
    ON unified_trade_data (TNVED2);

CREATE INDEX IF NOT EXISTS idx_fizob_napr_period
    ON fizob_index (NAPR, PERIOD);

CREATE INDEX IF NOT EXISTS idx_fizob_period
    ON fizob_index (PERIOD);

-- =============================================================================
-- 2. Облегчённый enriched (без dense_rank — он дорогой на 6.7M строк)
-- =============================================================================

CREATE OR REPLACE VIEW unified_trade_data_enriched_light AS
SELECT
    t.*,
    c.STRANA_NAME AS COUNTRY_NAME,
    t2.TNVED_NAME AS TNVED2_NAME,
    t4.TNVED_NAME AS TNVED4_NAME,
    h.TNVED4_NAME_SHORT AS TNVED4_NAME_SHORT,
    h.TNVED4_NAME_FULL AS TNVED4_NAME_FULL,
    t6.TNVED_NAME AS TNVED6_NAME,
    t8.TNVED_NAME AS TNVED8_NAME,
    COALESCE(t10.TNVED_NAME, t8.TNVED_NAME) AS TNVED_NAME,
    COALESCE(t10.TRANSLATED, t8.TRANSLATED) AS TNVED_TRANSLATED
FROM unified_trade_data AS t
LEFT JOIN country_reference AS c
    ON t.STRANA = c.STRANA
LEFT JOIN tnved_reference AS t2
    ON t.TNVED2 = t2.TNVED_CODE AND t2.TNVED_LEVEL = 2
LEFT JOIN tnved_reference AS t4
    ON t.TNVED4 = t4.TNVED_CODE AND t4.TNVED_LEVEL = 4
LEFT JOIN hs4_reference AS h
    ON t.TNVED4 = h.TNVED4
LEFT JOIN tnved_reference AS t6
    ON t.TNVED6 = t6.TNVED_CODE AND t6.TNVED_LEVEL = 6
LEFT JOIN tnved_reference AS t8
    ON t.TNVED8 = t8.TNVED_CODE AND t8.TNVED_LEVEL = 8
LEFT JOIN tnved_reference AS t10
    ON t.TNVED = t10.TNVED_CODE AND t10.TNVED_LEVEL = 10;

-- Опционально: материализовать enriched (быстрее для дашборда, больше места на диске).
-- Раскомментировать при необходимости:
--
-- DROP TABLE IF EXISTS unified_trade_data_enriched_light_mat;
-- CREATE TABLE unified_trade_data_enriched_light_mat AS
--     SELECT * FROM unified_trade_data_enriched_light;
-- CREATE INDEX IF NOT EXISTS idx_enriched_light_period
--     ON unified_trade_data_enriched_light_mat (PERIOD);
-- CREATE INDEX IF NOT EXISTS idx_enriched_light_napr_period
--     ON unified_trade_data_enriched_light_mat (NAPR, PERIOD);

-- =============================================================================
-- 3. Материализация trade_mom_kpi
--    (из базовой таблицы, без enriched — MoM нужны только агрегаты)
-- =============================================================================

DROP TABLE IF EXISTS trade_mom_kpi_mat;

CREATE TABLE trade_mom_kpi_mat AS
WITH base AS (
    SELECT
        PERIOD,
        NAPR,
        SOURCE,
        TNVED2,
        STRANA,
        SUM(STOIM) AS STOIM,
        SUM(NETTO) AS NETTO
    FROM unified_trade_data
    WHERE STOIM IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5
),
pairs AS (
    SELECT
        t.PERIOD,
        t.NAPR,
        t.SOURCE,
        t.TNVED2,
        t.STRANA,
        t.STOIM AS stoim_t,
        t1.STOIM AS stoim_t1,
        t.NETTO AS netto_t,
        t1.NETTO AS netto_t1,
        t1.PERIOD AS period_t1
    FROM base t
    JOIN base t1
      ON t.NAPR = t1.NAPR
     AND t.SOURCE = t1.SOURCE
     AND COALESCE(t.TNVED2, '') = COALESCE(t1.TNVED2, '')
     AND t.STRANA = t1.STRANA
     AND t1.PERIOD = t.PERIOD - INTERVAL 1 MONTH
),
comparable AS (
    SELECT *
    FROM pairs
    WHERE stoim_t1 > 0
),
comp_agg AS (
    SELECT
        PERIOD,
        period_t1,
        NAPR,
        SOURCE,
        TNVED2,
        COUNT(DISTINCT STRANA) AS n_comp_countries,
        SUM(stoim_t) AS stoim_t,
        SUM(stoim_t1) AS stoim_t1,
        SUM(COALESCE(netto_t, 0)) AS netto_t,
        SUM(COALESCE(netto_t1, 0)) AS netto_t1
    FROM comparable
    GROUP BY 1, 2, 3, 4, 5
),
total_t AS (
    SELECT
        PERIOD,
        NAPR,
        SOURCE,
        TNVED2,
        COUNT(DISTINCT STRANA) AS n_all_countries_t,
        SUM(STOIM) AS stoim_all_t
    FROM base
    GROUP BY 1, 2, 3, 4
)
SELECT
    c.PERIOD,
    c.period_t1,
    c.NAPR,
    c.SOURCE,
    c.TNVED2,
    c.n_comp_countries,
    t.n_all_countries_t,
    c.stoim_t,
    c.stoim_t1,
    c.netto_t,
    c.netto_t1,
    c.stoim_t / NULLIF(t.stoim_all_t, 0) AS coverage_stoim_t,
    CASE
        WHEN c.n_comp_countries < 3 THEN 'thin'
        WHEN c.stoim_t / NULLIF(t.stoim_all_t, 0) < 0.7 THEN 'low_coverage'
        ELSE 'ok'
    END AS quality_flag,
    (c.stoim_t / NULLIF(c.stoim_t1, 0)) - 1 AS mom_stoim,
    (c.netto_t / NULLIF(c.netto_t1, 0)) - 1 AS mom_netto
FROM comp_agg c
LEFT JOIN total_t t
  ON c.PERIOD = t.PERIOD
 AND c.NAPR = t.NAPR
 AND c.SOURCE = t.SOURCE
 AND COALESCE(c.TNVED2, '') = COALESCE(t.TNVED2, '');

CREATE INDEX IF NOT EXISTS idx_mom_kpi_period_napr
    ON trade_mom_kpi_mat (PERIOD, NAPR);

CREATE INDEX IF NOT EXISTS idx_mom_kpi_source
    ON trade_mom_kpi_mat (SOURCE);

-- =============================================================================
-- 4. Материализация coverage_matrix
-- =============================================================================

DROP TABLE IF EXISTS coverage_matrix_mat;

CREATE TABLE coverage_matrix_mat AS
WITH max_period AS (
    SELECT date_trunc('month', max(period)) AS max_month
    FROM unified_trade_data_enriched_light
    WHERE period IS NOT NULL
),
months AS (
    SELECT
        month_start,
        strftime(month_start, '%Y-%m') AS month_label
    FROM max_period,
         generate_series(
             max_month - INTERVAL 23 MONTH,
             max_month,
             INTERVAL 1 MONTH
         ) AS t(month_start)
),
countries AS (
    SELECT DISTINCT trim(country_name) AS country_name
    FROM unified_trade_data_enriched_light
    WHERE country_name IS NOT NULL
),
actual_data AS (
    SELECT
        trim(country_name) AS country_name,
        date_trunc('month', period) AS month_start
    FROM unified_trade_data_enriched_light, max_period
    WHERE country_name IS NOT NULL
      AND period IS NOT NULL
      AND period >= max_month - INTERVAL 23 MONTH
      AND period < max_month + INTERVAL 1 MONTH
    GROUP BY 1, 2
)
SELECT
    c.country_name,
    m.month_start,
    m.month_label,
    CASE WHEN a.month_start IS NOT NULL THEN 1 ELSE 0 END AS coverage
FROM countries c
CROSS JOIN months m
LEFT JOIN actual_data a
    ON c.country_name = a.country_name
   AND m.month_start = a.month_start;

CREATE INDEX IF NOT EXISTS idx_coverage_country_month
    ON coverage_matrix_mat (country_name, month_start);

-- =============================================================================
-- 5. Финализация
-- =============================================================================

CHECKPOINT;

-- Проверка:
SELECT 'unified_trade_data' AS obj, COUNT(*) AS rows FROM unified_trade_data
UNION ALL SELECT 'trade_mom_kpi_mat', COUNT(*) FROM trade_mom_kpi_mat
UNION ALL SELECT 'coverage_matrix_mat', COUNT(*) FROM coverage_matrix_mat;
