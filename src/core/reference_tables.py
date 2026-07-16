"""Reference table loaders and DuckDB reference-table writer."""

import json
import logging
from pathlib import Path
from typing import Dict, List

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


def build_unified_trade_data_enriched_view_sql() -> str:
    """SQL for the enriched trade view (shared by merge and DB slice utilities)."""
    return """
        CREATE OR REPLACE VIEW unified_trade_data_enriched AS
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


def refresh_hs4_reference(conn: duckdb.DuckDBPyConnection, project_root: Path) -> int:
    """
    Reload only hs4_reference from hs4_labels.json and refresh enriched view.

    Does not touch unified_trade_data or other reference tables.
    """
    logger.info("Refreshing hs4_reference...")
    hs4_df = load_hs4_labels(project_root)
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

    logger.info("Refreshing unified_trade_data_enriched view...")
    conn.execute(build_unified_trade_data_enriched_view_sql())
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
                SELECT DISTINCT TNVED_CODE, TNVED_LEVEL, TNVED_NAME, TRANSLATED
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

    # Create convenience view that joins main table with reference tables
    logger.info("Creating convenience view with joined reference data...")
    conn.execute(build_unified_trade_data_enriched_view_sql())
    logger.info("  ... created unified_trade_data_enriched view")

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

def load_tnved_mapping(project_root: Path) -> Dict[str, Dict[str, Dict[str, any]]]:
    """
    Loads TNVED code to name mappings from tnved.csv and missing_codes_translations.json.

    Returns a dictionary with structure:
    {
        'tnved2': {code: {'name': str, 'translated': bool}},
        'tnved4': {code: {'name': str, 'translated': bool}},
        ...
    }
    """
    mapping_file = project_root / "metadata" / "tnved.csv"
    translations_file = project_root / "metadata" / "translations" / "missing_codes_translations.json"

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

            for level in [2, 4, 6, 8, 10]:
                level_key = f'tnved{level}'
                level_data = df[df['LEVEL'] == level]
                for _, row in level_data.iterrows():
                    code = str(row['KOD']).strip()
                    name = str(row['NAME']).strip().upper()
                    mappings[level_key][code] = {
                        'name': name,
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
                            'translated': True
                        }

            if translations_count > 0:
                logger.info(f"Loaded {translations_count} translated TNVED codes from {translations_file}")
        except Exception as e:
            logger.error(f"Failed to load TNVED translations from {translations_file}: {e}")
    else:
        logger.warning(f"TNVED translations file not found at {translations_file}")

    return mappings


__all__ = [
    "build_unified_trade_data_enriched_view_sql",
    "load_partner_mapping",
    "load_strana_mapping",
    "load_hs4_labels",
    "load_tnved_mapping",
    "refresh_hs4_reference",
    "refresh_hs4_reference_db",
    "save_reference_tables",
]
