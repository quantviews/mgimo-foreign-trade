#!/usr/bin/env python3
"""
Script to verify that missing codes are actually missing from Russian TNVED reference.

This script:
1. Loads all missing codes from China, Turkey, and India
2. Loads all codes from Russian TNVED reference
3. Checks if any "missing" codes actually exist in the reference
4. Reports any false positives
"""

import pandas as pd
from pathlib import Path
import logging
from collections import defaultdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_russian_tnved_codes(tnved_file: Path) -> dict:
    """
    Load all TNVED codes from Russian reference file.
    
    Args:
        tnved_file: Path to metadata/tnved.csv
        
    Returns:
        Dictionary mapping level -> set of codes at that level
    """
    logger.info(f"Loading Russian TNVED codes from {tnved_file}")
    
    if not tnved_file.exists():
        logger.error(f"File does not exist: {tnved_file}")
        return {}
    
    try:
        df = pd.read_csv(tnved_file, dtype={'KOD': str, 'NAME': str, 'level': int})
        df.columns = df.columns.str.upper()
        
        # Group codes by level
        codes_by_level = defaultdict(set)
        
        for _, row in df.iterrows():
            kod = str(row['KOD']).strip()
            level = int(row['LEVEL'])
            
            # Normalize code to appropriate length based on level
            if level == 2:
                kod_normalized = kod.zfill(2)
            elif level == 4:
                kod_normalized = kod.zfill(4)
            elif level == 6:
                kod_normalized = kod.zfill(6)
            elif level == 8:
                kod_normalized = kod.zfill(8)
            elif level == 10:
                kod_normalized = kod.zfill(10)
            else:
                logger.warning(f"Unknown level {level} for code {kod}")
                continue
            
            codes_by_level[level].add(kod_normalized)
        
        # Also create a set of all codes at all levels for quick lookup
        all_codes = set()
        for level_codes in codes_by_level.values():
            all_codes.update(level_codes)
        
        logger.info(f"Loaded Russian TNVED codes:")
        for level in sorted(codes_by_level.keys()):
            logger.info(f"  Level {level}: {len(codes_by_level[level])} codes")
        logger.info(f"  Total unique codes: {len(all_codes)}")
        
        codes_by_level['all'] = all_codes
        return codes_by_level
        
    except Exception as e:
        logger.error(f"Failed to load Russian TNVED codes: {e}")
        return {}


def load_missing_codes(country_name: str, csv_file: Path) -> set:
    """
    Load missing codes from a CSV file.
    
    Args:
        country_name: Name of the country (for logging)
        csv_file: Path to CSV file with missing codes
        
    Returns:
        Set of TNVED codes
    """
    logger.info(f"Loading {country_name} missing codes from {csv_file}")
    
    if not csv_file.exists():
        logger.warning(f"File does not exist: {csv_file}")
        return set()
    
    try:
        df = pd.read_csv(csv_file, dtype={'TNVED': str})
        codes = set(str(row['TNVED']).strip() for _, row in df.iterrows())
        logger.info(f"  Loaded {len(codes)} codes from {country_name}")
        return codes
        
    except Exception as e:
        logger.error(f"Failed to load {country_name} codes: {e}")
        return set()


def verify_missing_codes(missing_codes: set, russian_codes_by_level: dict, country_name: str) -> dict:
    """
    Verify that missing codes are actually missing.
    
    Args:
        missing_codes: Set of codes reported as missing
        russian_codes_by_level: Dictionary mapping level -> set of codes
        country_name: Name of the country (for reporting)
        
    Returns:
        Dictionary with verification results
    """
    logger.info(f"Verifying {country_name} missing codes...")
    
    all_russian_codes = russian_codes_by_level.get('all', set())
    russian_codes_level_8 = russian_codes_by_level.get(8, set())
    
    # Check at level 10
    found_at_level_10 = []
    for code in missing_codes:
        if code in all_russian_codes:
            found_at_level_10.append(code)
    
    # Check at level 8
    found_at_level_8 = []
    missing_at_level_8 = []
    for code in missing_codes:
        code_at_level_8 = code[:8]
        if code_at_level_8 in russian_codes_level_8:
            found_at_level_8.append(code)
        else:
            missing_at_level_8.append(code)
    
    # Check at other levels
    found_at_levels = defaultdict(list)
    for level in [2, 4, 6]:
        russian_codes_at_level = russian_codes_by_level.get(level, set())
        for code in missing_codes:
            code_at_level = code[:level]
            if code_at_level in russian_codes_at_level:
                found_at_levels[level].append(code)
    
    results = {
        'total_missing': len(missing_codes),
        'found_at_level_10': found_at_level_10,
        'found_at_level_8': found_at_level_8,
        'missing_at_level_8': missing_at_level_8,
        'found_at_levels': dict(found_at_levels),
        'truly_missing_at_10': len(missing_codes) - len(found_at_level_10),
        'truly_missing_at_8': len(missing_at_level_8),
    }
    
    return results


def print_verification_report(china_results: dict, turkey_results: dict, india_results: dict,
                             china_codes: set, turkey_codes: set, india_codes: set,
                             output_file: Path = None):
    """
    Print verification report.
    
    Args:
        china_results: Verification results for China
        turkey_results: Verification results for Turkey
        india_results: Verification results for India
        china_codes: Set of China missing codes
        turkey_codes: Set of Turkey missing codes
        india_codes: Set of India missing codes
        output_file: Optional path to save report
    """
    report_lines = []
    report_lines.append("=" * 80)
    report_lines.append("VERIFICATION: Are Missing Codes Actually Missing?")
    report_lines.append("=" * 80)
    report_lines.append("")
    report_lines.append("This report checks if codes reported as 'missing' actually exist")
    report_lines.append("in the Russian TNVED reference at level 10 (full code).")
    report_lines.append("")
    
    for country_name, results, codes in [
        ('China', china_results, china_codes),
        ('Turkey', turkey_results, turkey_codes),
        ('India', india_results, india_codes),
    ]:
        report_lines.append(f"{country_name}:")
        report_lines.append(f"  Total codes reported as missing: {results['total_missing']:,}")
        report_lines.append("")
        report_lines.append(f"  Level 10 (full code):")
        report_lines.append(f"    Found at level 10 (FALSE POSITIVES): {len(results['found_at_level_10']):,}")
        report_lines.append(f"    Truly missing at level 10: {results['truly_missing_at_10']:,}")
        report_lines.append("")
        report_lines.append(f"  Level 8 (8-digit code):")
        report_lines.append(f"    Found at level 8 (parent category exists): {len(results['found_at_level_8']):,}")
        report_lines.append(f"    Missing at level 8 (no parent category): {results['truly_missing_at_8']:,}")
        report_lines.append("")
        
        if results['found_at_level_10']:
            report_lines.append(f"  ⚠️  WARNING: {len(results['found_at_level_10'])} codes were incorrectly reported as missing!")
            report_lines.append(f"  These codes actually EXIST in Russian TNVED reference:")
            report_lines.append("")
            for code in sorted(results['found_at_level_10'])[:50]:
                report_lines.append(f"    {code}")
            if len(results['found_at_level_10']) > 50:
                report_lines.append(f"    ... and {len(results['found_at_level_10']) - 50} more")
            report_lines.append("")
        
        if results['missing_at_level_8']:
            report_lines.append(f"  Codes missing at level 8 (first 20 examples):")
            for code in sorted(results['missing_at_level_8'])[:20]:
                report_lines.append(f"    {code} (8-digit: {code[:8]})")
            if len(results['missing_at_level_8']) > 20:
                report_lines.append(f"    ... and {len(results['missing_at_level_8']) - 20} more")
            report_lines.append("")
        
        # Check at other levels
        for level in [2, 4, 6]:
            if level in results['found_at_levels']:
                found = results['found_at_levels'][level]
                if found:
                    report_lines.append(f"  Codes with parent at level {level}: {len(found):,}")
                    report_lines.append("")
    
    report_text = "\n".join(report_lines)
    
    # Print to console
    try:
        print(report_text)
    except UnicodeEncodeError:
        logger.info("Report generated. See file for full details.")
        print(f"\nSummary:")
        print(f"  China: {china_results['total_missing']:,} missing, {len(china_results['found_at_level_10']):,} false positives")
        print(f"  Turkey: {turkey_results['total_missing']:,} missing, {len(turkey_results['found_at_level_10']):,} false positives")
        print(f"  India: {india_results['total_missing']:,} missing, {len(india_results['found_at_level_10']):,} false positives")
    
    # Save to file
    if output_file:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(report_text)
        logger.info(f"Report saved to {output_file}")


def main():
    """Main function."""
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    
    reports_dir = project_root / 'reports'
    metadata_dir = project_root / 'metadata'
    
    # Load Russian reference
    tnved_file = metadata_dir / 'tnved.csv'
    russian_codes_by_level = load_russian_tnved_codes(tnved_file)
    if not russian_codes_by_level:
        logger.error("Failed to load Russian codes. Exiting.")
        return
    
    # Load missing codes
    china_codes = load_missing_codes('China', reports_dir / 'china_tnved_missing_codes.csv')
    turkey_codes = load_missing_codes('Turkey', reports_dir / 'turkey_tnved_missing_codes.csv')
    india_codes = load_missing_codes('India', reports_dir / 'india_tnved_missing_codes.csv')
    
    # Verify
    china_results = verify_missing_codes(china_codes, russian_codes_by_level, 'China')
    turkey_results = verify_missing_codes(turkey_codes, russian_codes_by_level, 'Turkey')
    india_results = verify_missing_codes(india_codes, russian_codes_by_level, 'India')
    
    # Print report
    output_report = reports_dir / 'missing_codes_verification.txt'
    print_verification_report(
        china_results, turkey_results, india_results,
        china_codes, turkey_codes, india_codes,
        output_report
    )
    
    logger.info("Verification completed!")


if __name__ == "__main__":
    main()

