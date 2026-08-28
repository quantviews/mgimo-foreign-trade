"""Reference table loaders and DuckDB reference-table writer."""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


def _hs4_labels_paths(project_root: Path) -> List[Path]:
    """Candidate paths for curated HS4 short labels (metadata is canonical)."""
    return [
        project_root / "metadata" / "hs4_labels.json",
        project_root / "site" / "data" / "hs4_labels.json",
    ]


def load_hs4_labels(project_root: Path) -> pd.DataFrame:
    """
    Load curated HS4/TNVED4 short labels for charts and dashboards.

    Returns columns: TNVED4, TNVED4_NAME_SHORT, TNVED4_NAME_FULL.
    """
    empty = pd.DataFrame(
        columns=["TNVED4", "TNVED4_NAME_SHORT", "TNVED4_NAME_FULL"]
    )
    for path in _hs4_labels_paths(project_root):
        if not path.exists():
            continue
        try:
            with open(path, "r", encoding="utf-8") as handle:
                records = json.load(handle)
            if not records:
                logger.warning(f"HS4 labels file is empty: {path}")
                return empty

            raw = pd.DataFrame(records)
            code_col = "hs4" if "hs4" in raw.columns else "tnved4"
            if code_col not in raw.columns:
                logger.error(f"HS4 labels file missing hs4/tnved4 column: {path}")
                return empty

            name_short_col = "name_ru_short" if "name_ru_short" in raw.columns else None
            if name_short_col is None:
                logger.error(f"HS4 labels file missing name_ru_short column: {path}")
                return empty

            name_full_col = (
                "name_ru_full"
                if "name_ru_full" in raw.columns
                else name_short_col
            )

            labels = pd.DataFrame(
                {
                    "TNVED4": (
                        raw[code_col]
                        .astype(str)
                        .str.strip()
                        .str.replace(r"\D", "", regex=True)
                        .str.zfill(4)
                        .str[:4]
                    ),
                    "TNVED4_NAME_SHORT": raw[name_short_col].astype(str).str.strip(),
                    "TNVED4_NAME_FULL": raw[name_full_col].astype(str).str.strip(),
                }
            )
            labels = labels[labels["TNVED4"].str.len() == 4]
            labels = labels.drop_duplicates(subset=["TNVED4"], keep="first")
            logger.info(f"Loaded {len(labels)} HS4 labels from {path}")
            return labels
        except Exception as exc:
            logger.error(f"Failed to load HS4 labels from {path}: {exc}")
            return empty

    logger.warning(
        "HS4 labels file not found; hs4_reference will be created empty"
    )
    return empty


def _enriched_select_sql() -> str:
    """SELECT body for the enriched trade dataset (shared by view/table builders)."""
    return """
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
            COALESCE(t10.TNVED_UNIT, t8.TNVED_UNIT) AS TNVED_UNIT,
            COALESCE(t10.NAME_SOURCE, t8.NAME_SOURCE) AS TNVED_NAME_SOURCE,
            COALESCE(t10.TNVED_NAME_EN, t8.TNVED_NAME_EN) AS TNVED_NAME_EN,
            -- Ближайшее наименование из официального справочника. Обогащение
            -- берёт TNVED_NAME с самого глубокого уровня, и машинный перевод на
            -- 8 знаках выигрывает у официального текста на 6 — здесь наоборот:
            -- точность приносится в жертву проверяемости, а насколько именно,
            -- видно по TNVED_NAME_OFFICIAL_LEVEL.
            COALESCE(
                CASE WHEN t10.NAME_SOURCE <> 'mt' THEN t10.TNVED_NAME END,
                CASE WHEN t8.NAME_SOURCE  <> 'mt' THEN t8.TNVED_NAME  END,
                CASE WHEN t6.NAME_SOURCE  <> 'mt' THEN t6.TNVED_NAME  END,
                CASE WHEN t4.NAME_SOURCE  <> 'mt' THEN t4.TNVED_NAME  END,
                CASE WHEN t2.NAME_SOURCE  <> 'mt' THEN t2.TNVED_NAME  END
            ) AS TNVED_NAME_OFFICIAL,
            CASE
                WHEN t10.NAME_SOURCE <> 'mt' THEN 10
                WHEN t8.NAME_SOURCE  <> 'mt' THEN 8
                WHEN t6.NAME_SOURCE  <> 'mt' THEN 6
                WHEN t4.NAME_SOURCE  <> 'mt' THEN 4
                WHEN t2.NAME_SOURCE  <> 'mt' THEN 2
            END AS TNVED_NAME_OFFICIAL_LEVEL,
            COALESCE(t10.TRANSLATED, t8.TRANSLATED) AS TNVED_TRANSLATED,
            DENSE_RANK() OVER (
                PARTITION BY t.STRANA, t.TNVED, t.NAPR
                ORDER BY t.PERIOD DESC
            ) AS period_rank
        FROM unified_trade_data t
        LEFT JOIN country_reference c ON t.STRANA = c.STRANA
        LEFT JOIN tnved_reference t2 ON t.TNVED2 = t2.TNVED_CODE AND t2.TNVED_LEVEL = 2
        LEFT JOIN tnved_reference t4 ON t.TNVED4 = t4.TNVED_CODE AND t4.TNVED_LEVEL = 4
        LEFT JOIN hs4_reference h ON t.TNVED4 = h.TNVED4
        LEFT JOIN tnved_reference t6 ON t.TNVED6 = t6.TNVED_CODE AND t6.TNVED_LEVEL = 6
        LEFT JOIN tnved_reference t8 ON t.TNVED8 = t8.TNVED_CODE AND t8.TNVED_LEVEL = 8
        LEFT JOIN tnved_reference t10 ON t.TNVED = t10.TNVED_CODE AND t10.TNVED_LEVEL = 10
    """


def build_unified_trade_data_enriched_view_sql() -> str:
    """SQL creating the enriched dataset as a VIEW (recomputed on every query).

    Kept for lightweight consumers (e.g. DB slice utilities) that don't need a
    materialized copy. For the shipped Superset DB prefer the table builder below.
    """
    return "CREATE OR REPLACE VIEW unified_trade_data_enriched AS" + _enriched_select_sql()


def build_unified_trade_data_enriched_base_table_sql() -> str:
    """Lean fact table for the enriched dataset: base columns + the precomputed
    period_rank window, physically clustered by PERIOD/STRANA/NAPR.

    Name labels (TNVED*/country names) are NOT stored here — they are joined on demand
    by the view below. Those labels are pure functions of the codes and live in tiny
    reference tables (~15 MB); baking them across ~7M rows cost ~1.9 GB for joins that
    benchmark as free. The expensive DENSE_RANK window IS materialized so queries never
    recompute it.
    """
    return """
        CREATE OR REPLACE TABLE unified_trade_data_enriched_base AS
        SELECT
            t.*,
            DENSE_RANK() OVER (
                PARTITION BY t.STRANA, t.TNVED, t.NAPR
                ORDER BY t.PERIOD DESC
            ) AS period_rank
        FROM unified_trade_data t
        ORDER BY t.PERIOD, t.STRANA, t.NAPR
    """


def build_unified_trade_data_enriched_view_from_base_sql() -> str:
    """Enriched VIEW over the lean base table, joining reference tables for all name
    labels. Same columns and name as before, so Superset is unaffected — the labels
    just live in the view instead of on disk. Dim joins are cheap (reference tables are
    tiny) and the heavy window is already materialized in the base table.
    """
    return """
        CREATE OR REPLACE VIEW unified_trade_data_enriched AS
        SELECT
            b.* EXCLUDE (period_rank),
            c.STRANA_NAME AS COUNTRY_NAME,
            t2.TNVED_NAME AS TNVED2_NAME,
            t4.TNVED_NAME AS TNVED4_NAME,
            h.TNVED4_NAME_SHORT AS TNVED4_NAME_SHORT,
            h.TNVED4_NAME_FULL AS TNVED4_NAME_FULL,
            t6.TNVED_NAME AS TNVED6_NAME,
            t8.TNVED_NAME AS TNVED8_NAME,
            COALESCE(t10.TNVED_NAME, t8.TNVED_NAME) AS TNVED_NAME,
            COALESCE(t10.TNVED_UNIT, t8.TNVED_UNIT) AS TNVED_UNIT,
            COALESCE(t10.NAME_SOURCE, t8.NAME_SOURCE) AS TNVED_NAME_SOURCE,
            COALESCE(t10.TNVED_NAME_EN, t8.TNVED_NAME_EN) AS TNVED_NAME_EN,
            -- Ближайшее наименование из официального справочника. Обогащение
            -- берёт TNVED_NAME с самого глубокого уровня, и машинный перевод на
            -- 8 знаках выигрывает у официального текста на 6 — здесь наоборот:
            -- точность приносится в жертву проверяемости, а насколько именно,
            -- видно по TNVED_NAME_OFFICIAL_LEVEL.
            COALESCE(
                CASE WHEN t10.NAME_SOURCE <> 'mt' THEN t10.TNVED_NAME END,
                CASE WHEN t8.NAME_SOURCE  <> 'mt' THEN t8.TNVED_NAME  END,
                CASE WHEN t6.NAME_SOURCE  <> 'mt' THEN t6.TNVED_NAME  END,
                CASE WHEN t4.NAME_SOURCE  <> 'mt' THEN t4.TNVED_NAME  END,
                CASE WHEN t2.NAME_SOURCE  <> 'mt' THEN t2.TNVED_NAME  END
            ) AS TNVED_NAME_OFFICIAL,
            CASE
                WHEN t10.NAME_SOURCE <> 'mt' THEN 10
                WHEN t8.NAME_SOURCE  <> 'mt' THEN 8
                WHEN t6.NAME_SOURCE  <> 'mt' THEN 6
                WHEN t4.NAME_SOURCE  <> 'mt' THEN 4
                WHEN t2.NAME_SOURCE  <> 'mt' THEN 2
            END AS TNVED_NAME_OFFICIAL_LEVEL,
            COALESCE(t10.TRANSLATED, t8.TRANSLATED) AS TNVED_TRANSLATED,
            b.period_rank
        FROM unified_trade_data_enriched_base b
        LEFT JOIN country_reference c ON b.STRANA = c.STRANA
        LEFT JOIN tnved_reference t2 ON b.TNVED2 = t2.TNVED_CODE AND t2.TNVED_LEVEL = 2
        LEFT JOIN tnved_reference t4 ON b.TNVED4 = t4.TNVED_CODE AND t4.TNVED_LEVEL = 4
        LEFT JOIN hs4_reference h ON b.TNVED4 = h.TNVED4
        LEFT JOIN tnved_reference t6 ON b.TNVED6 = t6.TNVED_CODE AND t6.TNVED_LEVEL = 6
        LEFT JOIN tnved_reference t8 ON b.TNVED8 = t8.TNVED_CODE AND t8.TNVED_LEVEL = 8
        LEFT JOIN tnved_reference t10 ON b.TNVED = t10.TNVED_CODE AND t10.TNVED_LEVEL = 10
    """


def build_coverage_matrix_table_sql() -> str:
    """SQL materializing coverage_matrix: a 1/0 data-presence flag per country per
    month over the last 24 months.

    FACT ONLY — rows with TYPE='pred' (nowcast) are excluded, so the matrix reflects
    actually reported coverage rather than forecast gap-fill. The 24-month window end
    (max_month) is also derived from fact data, otherwise nowcast months beyond the
    last reported period would shift the window onto months that have no facts yet.

    Reads unified_trade_data + country_reference for COUNTRY_NAME (no dependency on the
    enriched view, which is now join-backed).
    """
    return """
        CREATE OR REPLACE TABLE coverage_matrix AS
        WITH fact AS (
            SELECT c.STRANA_NAME AS country_name, u.PERIOD AS period
            FROM unified_trade_data u
            LEFT JOIN country_reference c ON u.STRANA = c.STRANA
            WHERE u.TYPE = 'fact'
        ),
        max_period AS (
            SELECT date_trunc('month', max(period)) AS max_month
            FROM fact
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
            FROM fact
            WHERE country_name IS NOT NULL
        ),
        actual_data AS (
            SELECT
                trim(country_name) AS country_name,
                date_trunc('month', period) AS month_start
            FROM fact, max_period
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
           AND m.month_start = a.month_start
        ORDER BY c.country_name, m.month_start
    """


def build_trade_mom_kpi_table_sql() -> str:
    """SQL materializing trade_mom_kpi: month-over-month STOIM/NETTO change per
    (NAPR, SOURCE, TNVED2), aggregated over the countries comparable between the two
    months, plus a coverage-based quality_flag.

    Aggregates from base unified_trade_data (enriched labels not needed) and self-joins
    each month to the previous one. Clustered by PERIOD/NAPR for zonemap pruning.
    """
    return """
        CREATE OR REPLACE TABLE trade_mom_kpi AS
        WITH base AS (
            SELECT
                PERIOD, NAPR, SOURCE, TNVED2, STRANA,
                SUM(STOIM) AS STOIM,
                SUM(NETTO) AS NETTO
            FROM unified_trade_data
            WHERE STOIM IS NOT NULL
            GROUP BY 1, 2, 3, 4, 5
        ),
        pairs AS (
            SELECT
                t.PERIOD, t.NAPR, t.SOURCE, t.TNVED2, t.STRANA,
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
            SELECT * FROM pairs WHERE stoim_t1 > 0
        ),
        comp_agg AS (
            SELECT
                PERIOD, period_t1, NAPR, SOURCE, TNVED2,
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
                PERIOD, NAPR, SOURCE, TNVED2,
                COUNT(DISTINCT STRANA) AS n_all_countries_t,
                SUM(STOIM) AS stoim_all_t
            FROM base
            GROUP BY 1, 2, 3, 4
        )
        SELECT
            c.PERIOD, c.period_t1, c.NAPR, c.SOURCE, c.TNVED2,
            c.n_comp_countries,
            t.n_all_countries_t,
            c.stoim_t, c.stoim_t1, c.netto_t, c.netto_t1,
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
         AND COALESCE(c.TNVED2, '') = COALESCE(t.TNVED2, '')
        ORDER BY c.PERIOD, c.NAPR
    """


def build_fizob_enriched_table_sql() -> str:
    """SQL materializing fizob_enriched (the «Физобъемы» tab + native filters) from
    fizob_index_v joined to reference tables.

    Requires fizob_index_v to exist (fizob enabled during merge); callers must guard.

    Clustered by STRANA/NAPR/tn_level/tn_code/PERIOD — matches the typical fizob access
    pattern (a country+code time series) AND collapses the long name columns: with codes
    contiguous, DuckDB dictionary-compresses TNVED*_NAME instead of falling back to FSST,
    cutting the table ~2046 -> ~724 MB (TNVED4_NAME alone 939 -> 130 MB) while making that
    query ~9x faster (10 -> 1 ms). Do NOT reorder to PERIOD-first — it re-inflates the
    name columns back to FSST.
    """
    return """
        CREATE OR REPLACE TABLE fizob_enriched AS
        SELECT
            f.STRANA,
            c.STRANA_NAME AS COUNTRY_NAME,
            f.NAPR,
            f.PERIOD,
            EXTRACT(YEAR FROM f.PERIOD)::INTEGER AS YEAR,
            f.tn_level,
            f.tn_code,
            f.fizob,
            f.fizob_bp,
            f.idx,
            CASE WHEN f.tn_level = 2 THEN f.tn_code END AS TNVED2,
            CASE WHEN f.tn_level = 4 THEN f.tn_code END AS TNVED4,
            CASE WHEN f.tn_level = 6 THEN f.tn_code END AS TNVED6,
            t2.TNVED_NAME AS TNVED2_NAME,
            t4.TNVED_NAME AS TNVED4_NAME
        FROM fizob_index_v f
        LEFT JOIN country_reference c
            ON f.STRANA = c.STRANA
        LEFT JOIN tnved_reference t2
            ON f.tn_level = 2 AND f.tn_code = t2.TNVED_CODE AND t2.TNVED_LEVEL = 2
        LEFT JOIN tnved_reference t4
            ON f.tn_level = 4 AND f.tn_code = t4.TNVED_CODE AND t4.TNVED_LEVEL = 4
        ORDER BY f.STRANA, f.NAPR, f.tn_level, f.tn_code, f.PERIOD
    """


def refresh_hs4_reference(conn: duckdb.DuckDBPyConnection, project_root: Path) -> int:
    """
    Reload only hs4_reference from hs4_labels.json and refresh enriched view.

    Does not touch unified_trade_data or other reference tables.
    """
    logger.info("Refreshing hs4_reference...")
    hs4_df = load_hs4_labels(project_root)
    # The enriched view joins hs4_reference, so drop the view before replacing the table.
    conn.execute("DROP VIEW IF EXISTS unified_trade_data_enriched")
    conn.execute("DROP TABLE IF EXISTS hs4_reference")
    conn.register("hs4_ref_df", hs4_df)
    conn.execute("""
        CREATE TABLE hs4_reference AS
        SELECT DISTINCT TNVED4, TNVED4_NAME_SHORT, TNVED4_NAME_FULL
        FROM hs4_ref_df
        ORDER BY TNVED4
    """)
    conn.unregister("hs4_ref_df")
    conn.execute("CREATE INDEX idx_hs4_ref_tnved4 ON hs4_reference(TNVED4)")
    logger.info(f"  ... created hs4_reference table with {len(hs4_df)} rows")

    # Rebuild the enriched view; refreshed hs4 labels are picked up via the live join.
    logger.info("Rebuilding unified_trade_data_enriched view...")
    conn.execute(build_unified_trade_data_enriched_view_from_base_sql())
    logger.info("  ... updated unified_trade_data_enriched view")
    return len(hs4_df)


def refresh_hs4_reference_db(output_db_path: Path, project_root: Path) -> int:
    """Reload hs4_reference in an existing DuckDB file."""
    conn = duckdb.connect(str(output_db_path))
    try:
        return refresh_hs4_reference(conn, project_root)
    except Exception as e:
        logger.error(f"Failed to refresh hs4_reference: {e}")
        raise
    finally:
        conn.close()


def save_reference_tables(conn: duckdb.DuckDBPyConnection, project_root: Path):
    """
    Save reference tables (TNVED names, country names) as separate tables in DuckDB.
    This normalizes the database structure and reduces data duplication.

    Args:
        conn: DuckDB connection
        project_root: Path to project root for metadata loading
    """
    logger.info("Creating reference tables...")

    # Save TNVED mappings
    tnved_mappings = load_tnved_mapping(project_root)
    if tnved_mappings:
        # Create unified TNVED reference table
        tnved_refs = []
        for level_name, mapping in tnved_mappings.items():
            # Extract level number from key like 'tnved2', 'tnved10', etc.
            level_num = level_name.replace('tnved', '').replace('TNVED', '')
            try:
                level_int = int(level_num)
            except ValueError:
                logger.warning(f"Could not parse TNVED level from '{level_name}', skipping...")
                continue
            for code, code_data in mapping.items():
                # code_data is now a dict with 'name' and 'translated' keys
                name = code_data.get('name', '')
                translated = code_data.get('translated', False)
                unit = code_data.get('unit')
                name_source = code_data.get('source', NAME_SOURCE_FTS)
                name_en = code_data.get('name_en') or None

                if not name:
                    continue

                # Prepare code to match format in unified_trade_data
                # IMPORTANT: For ALL levels (2, 4, 6, 8, 10) - codes should match original structure (with leading zeros)
                # No normalization (removal of leading zeros) should be applied
                code_str = str(code).strip()

                # For all levels: use original code structure (with leading zeros)
                # First ensure code is at least 10 digits by padding with zeros on the RIGHT if needed
                if len(code_str) >= 10:
                    code_padded = code_str[:10]
                else:
                    code_padded = code_str + '0' * (10 - len(code_str))

                # Extract the appropriate length for this level (from original structure, preserving leading zeros)
                if level_int == 2:
                    normalized_code = code_padded[:2]
                elif level_int == 4:
                    normalized_code = code_padded[:4]
                elif level_int == 6:
                    normalized_code = code_padded[:6]
                elif level_int == 8:
                    normalized_code = code_padded[:8]
                elif level_int == 10:
                    normalized_code = code_padded[:10]
                else:
                    normalized_code = code_str

                tnved_refs.append({
                    'TNVED_CODE': normalized_code,
                    'TNVED_LEVEL': level_int,
                    'TNVED_NAME': name,
                    'TNVED_NAME_EN': name_en,
                    'TNVED_UNIT': unit,
                    'NAME_SOURCE': name_source,
                    'TRANSLATED': translated
                })

        if tnved_refs:
            tnved_df = pd.DataFrame(tnved_refs)
            # Remove duplicates, keeping official mappings (translated=False) over translations (translated=True)
            # Sort so that translated=False comes first, then drop duplicates
            tnved_df = tnved_df.sort_values('TRANSLATED').drop_duplicates(
                subset=['TNVED_CODE', 'TNVED_LEVEL'],
                keep='first'
            )

            conn.register('tnved_ref_df', tnved_df)
            conn.execute("""
                CREATE TABLE tnved_reference AS
                SELECT DISTINCT TNVED_CODE, TNVED_LEVEL, TNVED_NAME, TNVED_NAME_EN,
                       TNVED_UNIT, NAME_SOURCE, TRANSLATED
                FROM tnved_ref_df
                ORDER BY TNVED_LEVEL, TNVED_CODE
            """)
            conn.unregister('tnved_ref_df')

            official_count = (tnved_df['TRANSLATED'] == False).sum()
            translated_count = (tnved_df['TRANSLATED'] == True).sum()
            logger.info(f"  ... created tnved_reference table with {len(tnved_df)} rows "
                       f"({official_count} official, {translated_count} translated)")

            # Create index for faster joins
            conn.execute("CREATE INDEX idx_tnved_ref_code_level ON tnved_reference(TNVED_CODE, TNVED_LEVEL)")

    # Save country name mappings
    strana_mapping = load_strana_mapping(project_root)
    if strana_mapping:
        country_refs = [{'STRANA': k, 'STRANA_NAME': v} for k, v in strana_mapping.items()]
        country_df = pd.DataFrame(country_refs)
        conn.register('country_ref_df', country_df)
        conn.execute("""
            CREATE TABLE country_reference AS
            SELECT DISTINCT STRANA, STRANA_NAME
            FROM country_ref_df
            ORDER BY STRANA
        """)
        conn.unregister('country_ref_df')
        logger.info(f"  ... created country_reference table with {len(country_df)} rows")

        # Create index for faster joins
        conn.execute("CREATE INDEX idx_country_ref_strana ON country_reference(STRANA)")

    hs4_df = load_hs4_labels(project_root)
    conn.register("hs4_ref_df", hs4_df)
    conn.execute("""
        CREATE TABLE hs4_reference AS
        SELECT DISTINCT TNVED4, TNVED4_NAME_SHORT, TNVED4_NAME_FULL
        FROM hs4_ref_df
        ORDER BY TNVED4
    """)
    conn.unregister("hs4_ref_df")
    logger.info(f"  ... created hs4_reference table with {len(hs4_df)} rows")
    conn.execute("CREATE INDEX idx_hs4_ref_tnved4 ON hs4_reference(TNVED4)")

    # Enriched dataset = lean base table (codes + measures + period_rank window) plus a
    # view that joins reference tables for all name labels. Keeps the DB small without
    # losing any column or query speed (name joins benchmark as free; heavy window is
    # materialized). Names live in the view, not on disk.
    logger.info("Building unified_trade_data_enriched_base (lean, clustered)...")
    conn.execute(build_unified_trade_data_enriched_base_table_sql())
    logger.info("Creating unified_trade_data_enriched view (labels joined on demand)...")
    conn.execute(build_unified_trade_data_enriched_view_from_base_sql())
    logger.info("  ... enriched base table + view ready")

    # Materialize coverage_matrix (fact-only, last 24 months).
    logger.info("Materializing coverage_matrix (fact-only, last 24 months)...")
    conn.execute(build_coverage_matrix_table_sql())
    logger.info("  ... created coverage_matrix table")

    # Materialize trade_mom_kpi (month-over-month KPIs) from the base table.
    logger.info("Materializing trade_mom_kpi table...")
    conn.execute(build_trade_mom_kpi_table_sql())
    logger.info("  ... created trade_mom_kpi table")

    # Materialize fizob_enriched only when fizob_index_v exists (fizob may be
    # disabled with --no-fizob, in which case save_fizob_index never created it).
    fizob_view_exists = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'fizob_index_v'"
    ).fetchone()[0]
    if fizob_view_exists:
        logger.info("Materializing fizob_enriched table...")
        conn.execute(build_fizob_enriched_table_sql())
        logger.info("  ... created fizob_enriched table")
    else:
        logger.info("fizob_index_v not present (fizob disabled) — skipping fizob_enriched.")

def load_partner_mapping(project_root: Path) -> Dict[int, str]:
    """Loads Comtrade partner code (M49) to ISO2 mapping from JSON."""
    mapping_file = project_root / "metadata" / "comtrate-partnerAreas.json"
    if not mapping_file.exists():
        logger.error(f"Partner mapping file not found at {mapping_file}")
        return {}

    with open(mapping_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # M49 codes are numeric, ISO2 are strings
    mapping = {
        int(item['id']): item.get('PartnerCodeIsoAlpha2')
        for item in data.get('results', []) if item.get('PartnerCodeIsoAlpha2')
    }
    return mapping

def load_strana_mapping(project_root: Path) -> Dict[str, str]:
    """Loads ISO2 to country name mapping from STRANA.csv."""
    mapping_file = project_root / "metadata" / "STRANA.csv"
    if not mapping_file.exists():
        logger.error(f"Country name mapping file not found at {mapping_file}")
        return {}

    try:
        # Assuming the separator is a tab.
        df = pd.read_csv(mapping_file, sep='	', dtype=str)
        df.columns = df.columns.str.upper()
        # Create case-insensitive mapping: uppercase KOD (ISO2) -> NAME
        mapping = pd.Series(df.NAME.values, index=df.KOD.str.upper()).to_dict()
        logger.info(f"Loaded country name mapping for {len(mapping)} countries.")
        return mapping
    except Exception as e:
        logger.error(f"Failed to load country name mapping: {e}")
        return {}

# Дополнительные единицы измерения, приклеенные к началу NAME в справочнике ФТС
# ("ШТ-ЖИВЫЕ ЖИВОТНЫЕ", "КГ P2O5-ПЕНТАОКСИД ДИФОСФОРА"). Список закрытый: в тексте
# наименований дефис чаще принадлежит самому слову ("КРАФТ-БУМАГА", "2-НАФТОЛ"),
# поэтому отделяем только то, что действительно является единицей измерения.
TNVED_UNITS = frozenset({
    "ШТ", "100 ШТ", "1000 ШТ", "ПАР", "М", "М2", "М3", "1000 М3", "СМ3",
    "Л", "1000 Л", "Л 100% СПИРТА", "Г", "КГ", "КАР", "КИ", "БК",
    "КВТ", "КВТ*Ч", "1000 КВТ*Ч", "Л.С.", "Т ГП", "Г Д/И",
    "КГ N", "КГ K2O", "КГ P2O5", "КГ KOH", "КГ NAOH", "КГ H2O2",
    "КГ 90% С/В", "КГ U",
})

# Источники наименования (колонка NAME_SOURCE в tnved_reference).
NAME_SOURCE_FTS = "fts"  # справочник ФТС THBED.dbf, заморожен 09.02.2022
NAME_SOURCE_FNS = "fns"  # классификатор ТНВЭД ФНС, обновляемый
NAME_SOURCE_MT = "mt"    # машинный перевод наименования из зарубежного источника
NAME_SOURCE_MANUAL = "manual"  # выверенное вручную наименование, name_overrides.json


def split_unit_prefix(code: str, name: str) -> Tuple[Optional[str], str]:
    """Отделяет префикс дополнительной единицы измерения от наименования.

    Возвращает (единица или None, наименование). Единственное исключение из
    списка TNVED_UNITS — "М-" в группе 29: там это не метр, а локант в названии
    органического соединения (М-КСИЛОЛ, М-ФЕНИЛЕНДИАМИН).
    """
    text = name.replace("\xa0", " ").strip()  # NBSP встречается в 31 строке справочника
    idx = text.find("-")
    if idx < 1:
        return None, text
    prefix = text[:idx]
    if prefix not in TNVED_UNITS:
        return None, text
    if prefix == "М" and code.startswith("29"):
        return None, text
    return prefix, text[idx + 1:].strip()


def load_tnved_mapping(project_root: Path) -> Dict[str, Dict[str, Dict[str, any]]]:
    """
    Loads TNVED code to name mappings from tnved.csv and missing_codes_translations.json.

    Returns a dictionary with structure:
    {
        'tnved2': {code: {'name': str, 'unit': str|None, 'source': str, 'translated': bool}},
        'tnved4': {code: {...}},
        ...
    }

    `unit` — дополнительная единица измерения, отделённая от наименования;
    `source` — откуда взято наименование (см. NAME_SOURCE_*).
    """
    mapping_file = project_root / "metadata" / "tnved.csv"
    translations_file = project_root / "metadata" / "translations" / "missing_codes_translations.json"
    overrides_file = project_root / "metadata" / "translations" / "name_overrides.json"

    # Initialize mappings structure
    mappings = {
        'tnved2': {},
        'tnved4': {},
        'tnved6': {},
        'tnved8': {},
        'tnved10': {}
    }

    # Load official mappings from tnved.csv
    if mapping_file.exists():
        try:
            df = pd.read_csv(mapping_file, dtype={'KOD': str, 'NAME': str, 'level': int})
            df.columns = df.columns.str.upper()
            has_source = 'SOURCE' in df.columns

            for level in [2, 4, 6, 8, 10]:
                level_key = f'tnved{level}'
                level_data = df[df['LEVEL'] == level]
                for _, row in level_data.iterrows():
                    code = str(row['KOD']).strip()
                    unit, name = split_unit_prefix(code, str(row['NAME']).upper())
                    source = str(row['SOURCE']).strip() if has_source else NAME_SOURCE_FTS
                    mappings[level_key][code] = {
                        'name': name,
                        'name_en': None,
                        'unit': unit,
                        'source': source or NAME_SOURCE_FTS,
                        'translated': False
                    }

            logger.info("Successfully loaded official TNVED mappings for all levels.")
        except Exception as e:
            logger.error(f"Failed to load TNVED mapping from {mapping_file}: {e}")
    else:
        logger.warning(f"TNVED mapping file not found at {mapping_file}")

    # Load translations from missing_codes_translations_test.json
    if translations_file.exists():
        try:
            with open(translations_file, 'r', encoding='utf-8') as f:
                translations = json.load(f)

            translations_count = 0
            for code_10, data in translations.items():
                code_10_str = str(code_10).strip()
                russian_name = data.get('russian_name', '').strip().upper()
                # Английский оригинал, из которого сделан перевод, — единственный
                # способ проверить машинную подпись, поэтому он едет дальше.
                original_name = (data.get('original_name') or '').strip()

                if not russian_name:
                    continue

                # Pad code to 10 digits on the RIGHT if needed (never remove leading zeros)
                code_10_padded = code_10_str.strip()
                if len(code_10_padded) >= 10:
                    code_10_padded = code_10_padded[:10]
                else:
                    code_10_padded = code_10_padded + '0' * (10 - len(code_10_padded))

                # Add translation for level 10 (only if not already in official mappings)
                if code_10_padded not in mappings['tnved10']:
                    mappings['tnved10'][code_10_padded] = {
                        'name': russian_name,
                        'name_en': original_name,
                        'unit': None,
                        'source': NAME_SOURCE_MT,
                        'translated': True
                    }
                    translations_count += 1

                # Also add translations for parent levels (2, 4, 6, 8) if they don't exist
                # Extract parent codes from padded 10-digit code (preserving leading zeros)
                for level in [2, 4, 6, 8]:
                    level_key = f'tnved{level}'
                    # Extract first N digits from padded code
                    code_level = code_10_padded[:level]

                    # Only add if this level code doesn't exist in official mappings
                    if code_level not in mappings[level_key]:
                        # Use the russian_name from the 10-digit code as fallback
                        # Note: This is not ideal, but we don't have separate translations for parent levels
                        mappings[level_key][code_level] = {
                            'name': russian_name,  # Using the 10-digit name as fallback
                            'name_en': original_name,
                            'unit': None,
                            'source': NAME_SOURCE_MT,
                            'translated': True
                        }

            if translations_count > 0:
                logger.info(f"Loaded {translations_count} translated TNVED codes from {translations_file}")
        except Exception as e:
            logger.error(f"Failed to load TNVED translations from {translations_file}: {e}")
    else:
        logger.warning(f"TNVED translations file not found at {translations_file}")

    _apply_name_overrides(mappings, overrides_file)

    return mappings


def _apply_name_overrides(mappings: Dict[str, Dict[str, Dict[str, any]]], path: Path) -> None:
    """Накладывает выверенные вручную наименования поверх машинных.

    Правка десятизначного кода тянется на родительские уровни, но только туда,
    где лежит ровно та же машинная строка: загрузчик переводов копирует
    наименование конечного кода в отсутствующие 2/4/6/8, и чинить нужно все
    копии, не задевая чужие наименования.
    """
    if not path.exists():
        return
    try:
        with open(path, 'r', encoding='utf-8') as f:
            overrides = json.load(f)
    except Exception as exc:
        logger.error(f"Failed to load TNVED name overrides from {path}: {exc}")
        return

    applied = 0
    for code, data in overrides.items():
        name = (data.get('russian_name') or '').strip().upper()
        if not name:
            continue
        code = str(code).strip()
        replaced = (data.get('replaces') or '').strip().upper()
        for level in (10, 8, 6, 4, 2):
            entry = mappings[f'tnved{level}'].get(code[:level])
            if entry is None:
                continue
            if level < 10 and (entry.get('source') != NAME_SOURCE_MT
                               or entry.get('name') != replaced):
                continue
            entry['name'] = name
            entry['source'] = NAME_SOURCE_MANUAL
            entry['translated'] = False
            applied += 1

    if applied:
        logger.info(f"Applied {applied} manual TNVED name overrides from {path}")


__all__ = [
    "build_unified_trade_data_enriched_view_sql",
    "build_unified_trade_data_enriched_base_table_sql",
    "build_unified_trade_data_enriched_view_from_base_sql",
    "build_coverage_matrix_table_sql",
    "build_trade_mom_kpi_table_sql",
    "build_fizob_enriched_table_sql",
    "load_partner_mapping",
    "load_strana_mapping",
    "load_hs4_labels",
    "load_tnved_mapping",
    "refresh_hs4_reference",
    "refresh_hs4_reference_db",
    "save_reference_tables",
]
