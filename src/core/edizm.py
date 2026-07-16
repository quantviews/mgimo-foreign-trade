"""EDIZM reference loading helpers."""

import json
import logging
from pathlib import Path
from typing import Dict

import pandas as pd

from core.normalization_rules import (
    apply_special_edizm_cases,
    get_special_edizm_aliases,
    normalize_edizm_value,
    resolve_edizm_record,
    resolve_edizm_records,
    standardize_edizm_columns,
)

logger = logging.getLogger(__name__)


def load_edizm_mapping(project_root: Path) -> Dict[int, str]:
    """Loads Comtrade qtyCode to qtyAbbr mapping from JSON."""
    mapping_file = project_root / "metadata" / "comtradte-QuantityUnits.json"
    if not mapping_file.exists():
        logger.error(f"Edizm mapping file not found at {mapping_file}")
        return {}

    with open(mapping_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    mapping = {
        item['qtyCode']: item.get('qtyAbbr')
        for item in data.get('results', [])
    }
    return mapping

def load_common_edizm_mapping(project_root: Path) -> Dict[str, Dict[str, str]]:
    """Loads a comprehensive, case-insensitive mapping for EDIZM values."""
    mapping_file = project_root / "metadata" / "edizm.csv"
    if not mapping_file.exists():
        logger.error(f"Common EDIZM mapping file not found at {mapping_file}")
        return {}

    try:
        # Read all columns as strings and prevent pandas from interpreting "NA" as NaN
        df = pd.read_csv(mapping_file, dtype=str, na_filter=False)

        # Standardize column names and values to uppercase for case-insensitive matching
        df.columns = df.columns.str.upper()
        df['KOD'] = df['KOD'].str.replace('"', '').str.strip()
        df['NAME'] = df['NAME'].str.upper().str.strip()

        # Create canonical records from the main edizm file (vectorized)
        canonical_records = {}
        # Use itertuples for better performance than iterrows
        for row in df.itertuples(index=False):
            record = {'KOD': row.KOD, 'NAME': row.NAME}
            canonical_records[row.NAME] = record
            # Also map by KOD if it exists
            if row.KOD:
                canonical_records[row.KOD] = record

        final_mapping = {}
        # Populate mapping from the edizm file itself (KOD, NAME) - vectorized
        for row in df.itertuples(index=False):
            record = canonical_records[row.NAME]
            final_mapping[row.NAME] = record
            if row.KOD:
                final_mapping[row.KOD] = record

        # Add a comprehensive set of aliases. All keys must be uppercase.
        aliases = {
            # Russian abbreviations
            'ШТ': canonical_records.get('ШТУКА'),
            'КГ': canonical_records.get('КИЛОГРАММ'),
            'Т': canonical_records.get('ТОННА, МЕТРИЧЕСКАЯ ТОННА (1000 КГ)'),
            'М': canonical_records.get('МЕТР'),
            'М2': canonical_records.get('КВАДРАТНЫЙ МЕТР'),
            'М3': canonical_records.get('КУБИЧЕСКИЙ МЕТР'),
            'Л': canonical_records.get('ЛИТР'),
            'Г': canonical_records.get('ГРАММ'),
            'КАРАТ': canonical_records.get('МЕТРИЧЕСКИЙ КАРАТ(1КАРАТ=2*10(-4)КГ'),

            # Comtrade abbreviations (from comtradte-QuantityUnits.json)
            'KG': canonical_records.get('КИЛОГРАММ'),
            'U': canonical_records.get('ШТУКА'),
            'L': canonical_records.get('ЛИТР'),
            'M': canonical_records.get('МЕТР'),  # Latin M (meter)
            'M²': canonical_records.get('КВАДРАТНЫЙ МЕТР'),
            'M2': canonical_records.get('КВАДРАТНЫЙ МЕТР'),  # M2 without superscript
            'M3': canonical_records.get('КУБИЧЕСКИЙ МЕТР'),
            '2U': canonical_records.get('ПАРА'),
            'CARAT': canonical_records.get('МЕТРИЧЕСКИЙ КАРАТ(1КАРАТ=2*10(-4)КГ'),
            '1000U': canonical_records.get('ТЫСЯЧА ШТУК'),
            'G': canonical_records.get('ГРАММ'),
            '1000 KWH': canonical_records.get('1000 КИЛОВАТТ-ЧАС'),
            '1000 L': canonical_records.get('1000 ЛИТРОВ'),
            '1000 KG': canonical_records.get('ТОННА, МЕТРИЧЕСКАЯ ТОННА (1000 КГ)'),
            'L ALC 100%': canonical_records.get('ЛИТР ЧИСТОГО (100%) СПИРТА'),
            # Additional Comtrade codes
            'BBL': canonical_records.get('БАРРЕЛЬ'),  # Barrel (code 11, if exists)
            'CT/L': canonical_records.get('ТОННА ГРУЗОПОДЪЕМНОСТИ'),  # Carrying capacity in tonnes (code 36, if exists)
            '12U': canonical_records.get('ШТУКА'),  # 12 units (approximate to piece)
            'KG/NET EDA': canonical_records.get('КИЛОГРАММ'),  # Variant with / instead of space
            'KG MET.AM.': canonical_records.get('КИЛОГРАММ МЕТАЛЛИЧЕСКОГО АММИАКА'),  # Kilogram of metallic ammonium (if exists)
            'GI F/S': canonical_records.get('ГРАММ ДЕЛЯЩИХСЯ ИЗОТОПОВ'),  # Gram of fissile isotopes (code 38, if exists)
            'U (JEU/PACK)': canonical_records.get('УПАКОВКА') or canonical_records.get('ШТУКА'),  # Number of packages (code 10, if exists)
            'U JEU/PACK': canonical_records.get('УПАКОВКА') or canonical_records.get('ШТУКА'),  # Number of packages (without parentheses)
            'KG U': canonical_records.get('КИЛОГРАММ УРАНА'),  # Kilogram of uranium (code 35)
            'GT': canonical_records.get('ВАЛОВАЯ РЕГИСТРОВАЯ ВМЕСТИМОСТЬ'),  # Gross tonnage (code 40, if exists)
            'GRT': canonical_records.get('ВАЛОВАЯ РЕГИСТРОВАЯ ВМЕСТИМОСТЬ'),  # Gross register ton (code 39, if exists)

            # Other observed values from logs
            'KG NET EDA': canonical_records.get('КИЛОГРАММ'),
            'Л 100% СПИРТА': canonical_records.get('ЛИТР ЧИСТОГО (100%) СПИРТА'),
            'КГ NAOH': canonical_records.get('КИЛОГРАММ ГИДРОКСИДА НАТРИЯ'),
            'КГ KOH': canonical_records.get('КИЛОГРАММ ГИДРОКСИДА КАЛИЯ'),
            'КГ N': canonical_records.get('КИЛОГРАММ АЗОТА'),
            'КГ K2O': canonical_records.get('КИЛОГРАММ ОКСИДА КАЛИЯ'),
            'КГ P2O5': canonical_records.get('КИЛОГРАММ ПЯТИОКИСИ ФОСФОРА'),
            'КГ H2O2': canonical_records.get('КИЛОГРАММ ПЕРОКСИДА ВОДОРОДА'),
            'КГ 90 %-ГО СУХОГО ВЕЩЕСТВА': canonical_records.get('КИЛОГРАММ 90 %-ГО СУХОГО ВЕЩЕСТВА'),
            'КГ U': canonical_records.get('КИЛОГРАММ УРАНА'),

            # Additional variants and common misspellings
            'M³': canonical_records.get('КУБИЧЕСКИЙ МЕТР'),  # Superscript 3
            'M3': canonical_records.get('КУБИЧЕСКИЙ МЕТР'),  # Already exists, but ensure it's there
            'М³': canonical_records.get('КУБИЧЕСКИЙ МЕТР'),  # Cyrillic with superscript
            'KG H2O2': canonical_records.get('КИЛОГРАММ ПЕРОКСИДА ВОДОРОДА'),  # Latin version
            'KG N': canonical_records.get('КИЛОГРАММ АЗОТА'),  # Latin version
            'КГ 90% С/В': canonical_records.get('КИЛОГРАММ 90 %-ГО СУХОГО ВЕЩЕСТВА'),  # Variant with /В
            'КГ 90% СВ': canonical_records.get('КИЛОГРАММ 90 %-ГО СУХОГО ВЕЩЕСТВА'),  # Variant without /
            '1000 ШТ': canonical_records.get('ТЫСЯЧА ШТУК'),  # Russian version
            '100 ШТ': canonical_records.get('ШТУКА'),  # 100 pieces = pieces (approximate)
            'КАР': canonical_records.get('МЕТРИЧЕСКИЙ КАРАТ(1КАРАТ=2*10(-4)КГ'),  # Abbreviation
            'ЭЛЕМ': canonical_records.get('ШТУКА'),  # Element/piece (approximate)

            # Additional Comtrade abbreviations and variants
            'KG NAOH': canonical_records.get('КИЛОГРАММ ГИДРОКСИДА НАТРИЯ'),  # Sodium hydroxide
            'KG KOH': canonical_records.get('КИЛОГРАММ ГИДРОКСИДА КАЛИЯ'),  # Potassium hydroxide
            'KG K2O': canonical_records.get('КИЛОГРАММ ОКСИДА КАЛИЯ'),  # Potassium oxide
            'KG P2O5': canonical_records.get('КИЛОГРАММ ПЯТИОКИСИ ФОСФОРА'),  # Phosphorus pentoxide
            'KG 90% SDT': canonical_records.get('КИЛОГРАММ 90 %-ГО СУХОГО ВЕЩЕСТВА'),  # 90% dry substance (SDT variant)
            'KG 90% SD': canonical_records.get('КИЛОГРАММ 90 %-ГО СУХОГО ВЕЩЕСТВА'),  # 90% dry substance (SD variant)
            '1000 M3': canonical_records.get('1000 КУБИЧЕСКИХ МЕТРОВ'),  # 1000 cubic meters (if exists)
            'CE/EL': canonical_records.get('ЭЛЕМЕНТ') or canonical_records.get('ШТУКА'),  # Number of cells/elements
            'CE EL': canonical_records.get('ЭЛЕМЕНТ') or canonical_records.get('ШТУКА'),  # Number of cells/elements (space variant)
            'TJ': canonical_records.get('ТЕРАДЖОУЛЬ'),  # Terajoule (if exists)
            'TERAJOULE': canonical_records.get('ТЕРАДЖОУЛЬ'),  # Terajoule full name (if exists)
        }
        aliases.update(get_special_edizm_aliases(canonical_records))

        final_mapping.update(aliases)

        # Filter out any None values that may have resulted from missing keys
        final_mapping = {k: v for k, v in final_mapping.items() if v is not None and k is not None}

        # Diagnostic: Check if БЕККЕРЕЛЬ was loaded
        if 'БЕККЕРЕЛЬ' in canonical_records:
            logger.info(f"  - 'БЕККЕРЕЛЬ' found in canonical_records")
            if 'BQ' in final_mapping:
                logger.info(f"  - 'BQ' alias successfully added to final_mapping")
            else:
                logger.warning(f"  - 'BQ' alias NOT found in final_mapping (canonical_records.get returned None)")
        else:
            logger.warning(f"  - 'БЕККЕРЕЛЬ' NOT found in canonical_records")

        logger.info(f"Loaded common EDIZM mapping with {len(final_mapping)} case-insensitive keys.")
        return final_mapping
    except Exception as e:
        logger.error(f"Failed to load common EDIZM mapping: {e}")
        return {}


__all__ = [
    "apply_special_edizm_cases",
    "get_special_edizm_aliases",
    "load_common_edizm_mapping",
    "load_edizm_mapping",
    "normalize_edizm_value",
    "resolve_edizm_record",
    "resolve_edizm_records",
    "standardize_edizm_columns",
]
