#!/usr/bin/env python3
"""
Script to compare TNVED codes from Turkey data with Russian TNVED reference.

This script:
1. Loads all unique TNVED codes from Turkey JSON files (data_raw/turkey/hs_codes_json/turkey_codes*.json)
2. Loads all TNVED codes from Russian reference (metadata/tnved.csv)
3. Compares them and identifies codes present in Turkey data but missing in Russian reference
4. Provides statistics and detailed report
"""

import pandas as pd
import json
from pathlib import Path
import logging
from collections import defaultdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_turkey_tnved_codes(turkey_codes_dir: Path) -> tuple[set, dict]:
    """
    Load all unique TNVED codes and their names from Turkey JSON files.
    
    Turkey JSON files have format: {"01012100": "Pure-bred breeding horses", ...}
    where keys are 8-digit HS codes and values are commodity names.
    
    Args:
        turkey_codes_dir: Path to data_raw/turkey/hs_codes_json directory
        
    Returns:
        Tuple of (set of unique TNVED codes normalized to 10 digits, 
                 dict mapping normalized code -> commodity_name)
    """
    logger.info(f"Loading Turkey TNVED codes from {turkey_codes_dir}")
    
    if not turkey_codes_dir.exists():
        logger.error(f"Directory does not exist: {turkey_codes_dir}")
        return set(), {}
    
    turkey_codes = set()
    code_names = {}  # Mapping: normalized_code -> commodity_name
    json_files = sorted(turkey_codes_dir.glob("turkey_codes*.json"))
    
    if not json_files:
        logger.warning(f"No JSON files found in {turkey_codes_dir}")
        return set(), {}
    
    logger.info(f"Found {len(json_files)} JSON files to process")
    
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Turkey JSON files are dictionaries with HS8 codes as keys
            # Normalize 8-digit HS codes to 10-digit TNVED codes (add two zeros on the right)
            for hs8_code, commodity_name in data.items():
                original_code = str(hs8_code).strip()
                
                # Ensure code is 8 digits (pad with zeros on the left if needed)
                if len(original_code) < 8:
                    original_code = original_code.zfill(8)
                elif len(original_code) > 8:
                    logger.warning(f"Code {original_code} in {json_file.name} has length {len(original_code)}, expected 8. Truncating.")
                    original_code = original_code[:8]
                
                # Normalize to 10 digits by adding two zeros on the right
                tnved_code = original_code + '00'
                turkey_codes.add(tnved_code)
                
                # Store commodity name if available
                if commodity_name:
                    # Keep the most recent name if code appears in multiple files
                    code_names[tnved_code] = str(commodity_name).strip()
            
            logger.info(f"  Processed {json_file.name}: {len(data)} records")
            
        except Exception as e:
            logger.error(f"Failed to process {json_file.name}: {e}")
            continue
    
    logger.info(f"Total unique Turkey TNVED codes: {len(turkey_codes)}")
    logger.info(f"Total codes with names: {len(code_names)}")
    
    # Debug: show sample codes
    sample_codes = sorted(list(turkey_codes))[:10]
    logger.info(f"Sample Turkey codes: {sample_codes}")
    
    return turkey_codes, code_names


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
        
        # Debug: show sample codes at each level
        for level in [2, 4, 6, 8, 10]:
            if level in codes_by_level:
                sample = sorted(list(codes_by_level[level]))[:5]
                logger.info(f"  Sample Level {level} codes: {sample}")
        
        codes_by_level['all'] = all_codes
        return codes_by_level
        
    except Exception as e:
        logger.error(f"Failed to load Russian TNVED codes: {e}")
        return {}


def extract_code_at_level(code: str, level: int) -> str:
    """
    Extract code at specific level from a 10-digit TNVED code.
    
    Args:
        code: 10-digit TNVED code
        level: Level to extract (2, 4, 6, 8, or 10)
        
    Returns:
        Code at specified level
    """
    if level == 2:
        return code[:2]
    elif level == 4:
        return code[:4]
    elif level == 6:
        return code[:6]
    elif level == 8:
        return code[:8]
    elif level == 10:
        return code
    else:
        raise ValueError(f"Invalid level: {level}")


def compare_codes(turkey_codes: set, russian_codes_by_level: dict) -> dict:
    """
    Compare Turkey codes with Russian codes and find missing ones.
    
    Args:
        turkey_codes: Set of Turkey TNVED codes (10-digit)
        russian_codes_by_level: Dictionary mapping level -> set of codes
        
    Returns:
        Dictionary with comparison results
    """
    logger.info("Comparing codes...")
    
    # Debug: show sample codes for comparison
    sample_turkey = sorted(list(turkey_codes))[:3]
    logger.info(f"Sample Turkey codes for comparison: {sample_turkey}")
    for level in [2, 4, 6, 8, 10]:
        if level in russian_codes_by_level:
            sample_russian = sorted(list(russian_codes_by_level[level]))[:3]
            logger.info(f"Sample Russian Level {level} codes: {sample_russian}")
            # Show what Turkey codes would look like at this level
            sample_turkey_at_level = [extract_code_at_level(c, level) for c in sample_turkey]
            logger.info(f"Sample Turkey codes at Level {level}: {sample_turkey_at_level}")
    
    results = {
        'missing_full': [],  # Codes missing at level 10
        'missing_by_level': defaultdict(list),  # Codes missing at each level
        'found_at_level': defaultdict(int),  # Count of codes found at each level
        'statistics': {}
    }
    
    all_russian_codes = russian_codes_by_level.get('all', set())
    
    for turkey_code in sorted(turkey_codes):
        # Check if full 10-digit code exists
        if turkey_code not in all_russian_codes:
            results['missing_full'].append(turkey_code)
        
        # Check at each level
        for level in [2, 4, 6, 8, 10]:
            code_at_level = extract_code_at_level(turkey_code, level)
            russian_codes_at_level = russian_codes_by_level.get(level, set())
            
            if code_at_level in russian_codes_at_level:
                results['found_at_level'][level] += 1
            else:
                results['missing_by_level'][level].append(turkey_code)
    
    # Calculate statistics
    total_turkey_codes = len(turkey_codes)
    results['statistics'] = {
        'total_turkey_codes': total_turkey_codes,
        'missing_full_count': len(results['missing_full']),
        'missing_full_percent': (len(results['missing_full']) / total_turkey_codes * 100) if total_turkey_codes > 0 else 0,
        'found_at_levels': dict(results['found_at_level']),
        'missing_at_levels': {level: len(codes) for level, codes in results['missing_by_level'].items()}
    }
    
    return results


def save_missing_codes_csv(results: dict, code_names: dict, output_csv: Path):
    """
    Save missing codes to CSV file for further analysis.
    
    Args:
        results: Comparison results dictionary
        code_names: Dictionary mapping normalized code -> commodity_name
        output_csv: Path to output CSV file
    """
    logger.info(f"Saving missing codes to {output_csv}")
    
    missing_data = []
    for code in results['missing_full']:
        missing_data.append({
            'TNVED': code,
            'TNVED2': code[:2],
            'TNVED4': code[:4],
            'TNVED6': code[:6],
            'TNVED8': code[:8],
            'HS_NAME': code_names.get(code, ''),
            'Missing_at_level_2': code[:2] not in results.get('russian_codes_level_2', set()),
            'Missing_at_level_4': code[:4] not in results.get('russian_codes_level_4', set()),
            'Missing_at_level_6': code[:6] not in results.get('russian_codes_level_6', set()),
            'Missing_at_level_8': code[:8] not in results.get('russian_codes_level_8', set()),
            'Missing_at_level_10': True,
        })
    
    if missing_data:
        df = pd.DataFrame(missing_data)
        # Reorder columns to put HS_NAME after TNVED
        cols = ['TNVED', 'HS_NAME', 'TNVED2', 'TNVED4', 'TNVED6', 'TNVED8',
                'Missing_at_level_2', 'Missing_at_level_4', 'Missing_at_level_6',
                'Missing_at_level_8', 'Missing_at_level_10']
        df = df[cols]
        output_csv.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_csv, index=False, encoding='utf-8-sig')
        logger.info(f"Saved {len(missing_data)} missing codes to CSV")
        logger.info(f"  Codes with names: {sum(1 for d in missing_data if d['HS_NAME'])}")
    else:
        logger.info("No missing codes to save")


def print_report(results: dict, turkey_codes: set, output_file: Path = None):
    """
    Print comparison report.
    
    Args:
        results: Comparison results dictionary
        turkey_codes: Set of all Turkey codes (for group analysis)
        output_file: Optional path to save report to file
    """
    stats = results['statistics']
    
    report_lines = []
    report_lines.append("=" * 80)
    report_lines.append("COMPARISON REPORT: Turkey TNVED Codes vs Russian Reference")
    report_lines.append("=" * 80)
    report_lines.append("")
    
    report_lines.append("SUMMARY STATISTICS:")
    report_lines.append(f"  Total Turkey TNVED codes: {stats['total_turkey_codes']:,}")
    report_lines.append(f"  Missing at level 10 (full code): {stats['missing_full_count']:,} ({stats['missing_full_percent']:.2f}%)")
    report_lines.append("")
    
    report_lines.append("CODES FOUND AT EACH LEVEL:")
    for level in [2, 4, 6, 8, 10]:
        found_count = stats['found_at_levels'].get(level, 0)
        total = stats['total_turkey_codes']
        percent = (found_count / total * 100) if total > 0 else 0
        report_lines.append(f"  Level {level}: {found_count:,} / {total:,} ({percent:.2f}%)")
    report_lines.append("")
    
    report_lines.append("CODES MISSING AT EACH LEVEL:")
    for level in [2, 4, 6, 8, 10]:
        missing_count = stats['missing_at_levels'].get(level, 0)
        total = stats['total_turkey_codes']
        percent = (missing_count / total * 100) if total > 0 else 0
        report_lines.append(f"  Level {level}: {missing_count:,} / {total:,} ({percent:.2f}%)")
    report_lines.append("")
    
    # Show sample of missing codes at each level
    report_lines.append("SAMPLE OF MISSING CODES (first 20 at each level):")
    for level in [2, 4, 6, 8, 10]:
        missing_codes = results['missing_by_level'][level][:20]
        if missing_codes:
            report_lines.append(f"  Level {level}: {', '.join(missing_codes)}")
            if len(results['missing_by_level'][level]) > 20:
                report_lines.append(f"    ... and {len(results['missing_by_level'][level]) - 20} more")
    report_lines.append("")
    
    # Analysis of missing codes by groups
    report_lines.append("=" * 80)
    report_lines.append("ANALYSIS OF MISSING CODES BY GROUPS")
    report_lines.append("=" * 80)
    
    # Group missing codes by first 2 digits
    missing_by_group_2 = defaultdict(list)
    for code in results['missing_full']:
        group = code[:2]
        missing_by_group_2[group].append(code)
    
    report_lines.append("\nMissing codes grouped by first 2 digits (Level 2 groups):")
    for group in sorted(missing_by_group_2.keys()):
        count = len(missing_by_group_2[group])
        total_turkey_in_group = sum(1 for c in turkey_codes if c.startswith(group))
        percent = (count / total_turkey_in_group * 100) if total_turkey_in_group > 0 else 0
        report_lines.append(f"  Group {group}: {count:,} missing out of {total_turkey_in_group:,} total ({percent:.1f}%)")
    
    # Show top groups with most missing codes
    report_lines.append("\nTop 10 groups with most missing codes:")
    sorted_groups = sorted(missing_by_group_2.items(), key=lambda x: len(x[1]), reverse=True)
    for group, codes in sorted_groups[:10]:
        report_lines.append(f"  Group {group}: {len(codes):,} missing codes")
        # Show sample codes from this group
        sample = sorted(codes)[:5]
        report_lines.append(f"    Sample: {', '.join(sample)}")
    
    # Detailed list of missing full codes (only for groups with few codes)
    report_lines.append("\n" + "=" * 80)
    report_lines.append("DETAILED LIST OF MISSING FULL CODES (Level 10)")
    report_lines.append("=" * 80)
    report_lines.append("Showing groups with <= 20 missing codes for detailed view:\n")
    
    for group in sorted(missing_by_group_2.keys()):
        codes_in_group = sorted(missing_by_group_2[group])
        if len(codes_in_group) <= 20:
            report_lines.append(f"Group {group} ({len(codes_in_group)} codes):")
            # Print codes in columns of 5
            for i in range(0, len(codes_in_group), 5):
                chunk = codes_in_group[i:i+5]
                report_lines.append("  " + "  ".join(chunk))
            report_lines.append("")
    
    # For larger groups, just show summary
    large_groups = [(g, codes) for g, codes in missing_by_group_2.items() if len(codes) > 20]
    if large_groups:
        report_lines.append("\nGroups with > 20 missing codes (see CSV file for full list):")
        for group, codes in sorted(large_groups, key=lambda x: len(x[1]), reverse=True):
            report_lines.append(f"  Group {group}: {len(codes):,} missing codes")
    
    report_text = "\n".join(report_lines)
    
    # Print to console
    print(report_text)
    
    # Save to file if specified
    if output_file:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(report_text)
        logger.info(f"Report saved to {output_file}")


def main():
    """Main function."""
    # Define paths relative to script location
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    
    turkey_codes_dir = project_root / 'data_raw' / 'turkey' / 'hs_codes_json'
    tnved_file = project_root / 'metadata' / 'tnved.csv'
    output_report = project_root / 'reports' / 'turkey_tnved_comparison.txt'
    
    logger.info("Starting TNVED codes comparison...")
    
    # Load codes and names
    turkey_codes, code_names = load_turkey_tnved_codes(turkey_codes_dir)
    if not turkey_codes:
        logger.error("No Turkey codes loaded. Exiting.")
        return
    
    russian_codes_by_level = load_russian_tnved_codes(tnved_file)
    if not russian_codes_by_level:
        logger.error("No Russian codes loaded. Exiting.")
        return
    
    # Compare
    results = compare_codes(turkey_codes, russian_codes_by_level)
    
    # Store reference codes in results for CSV export
    for level in [2, 4, 6, 8, 10]:
        results[f'russian_codes_level_{level}'] = russian_codes_by_level.get(level, set())
    
    # Print report
    print_report(results, turkey_codes, output_report)
    
    # Save missing codes to CSV
    output_csv = project_root / 'reports' / 'turkey_tnved_missing_codes.csv'
    save_missing_codes_csv(results, code_names, output_csv)
    
    logger.info("Comparison completed!")


if __name__ == "__main__":
    main()

