#!/usr/bin/env python3
"""
Script to compare missing TNVED codes across China, Turkey, and India.

This script:
1. Loads missing codes from all three countries
2. Compares codes to find common and unique ones
3. Compares names for common codes to see if they differ
4. Provides detailed statistics and analysis
"""

import pandas as pd
from pathlib import Path
import logging
from collections import defaultdict
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_missing_codes(country_name: str, csv_file: Path) -> dict:
    """
    Load missing codes from a CSV file.
    
    Args:
        country_name: Name of the country (for logging)
        csv_file: Path to CSV file with missing codes
        
    Returns:
        Dictionary mapping TNVED code -> HS_NAME
    """
    logger.info(f"Loading {country_name} missing codes from {csv_file}")
    
    if not csv_file.exists():
        logger.warning(f"File does not exist: {csv_file}")
        return {}
    
    try:
        df = pd.read_csv(csv_file, dtype={'TNVED': str})
        codes_dict = {}
        
        for _, row in df.iterrows():
            code = str(row['TNVED']).strip()
            name = row.get('HS_NAME', '')
            if pd.notna(name):
                codes_dict[code] = str(name).strip()
            else:
                codes_dict[code] = ''
        
        logger.info(f"  Loaded {len(codes_dict)} codes from {country_name}")
        return codes_dict
        
    except Exception as e:
        logger.error(f"Failed to load {country_name} codes: {e}")
        return {}


def compare_codes(china_codes: dict, turkey_codes: dict, india_codes: dict) -> dict:
    """
    Compare codes across all three countries.
    
    Args:
        china_codes: Dictionary of China codes
        turkey_codes: Dictionary of Turkey codes
        india_codes: Dictionary of India codes
        
    Returns:
        Dictionary with comparison results
    """
    logger.info("Comparing codes across countries...")
    
    china_set = set(china_codes.keys())
    turkey_set = set(turkey_codes.keys())
    india_set = set(india_codes.keys())
    
    # Find intersections
    all_three = china_set & turkey_set & india_set
    china_turkey = china_set & turkey_set - india_set
    china_india = china_set & india_set - turkey_set
    turkey_india = turkey_set & india_set - china_set
    
    # Unique to each country
    china_only = china_set - turkey_set - india_set
    turkey_only = turkey_set - china_set - india_set
    india_only = india_set - china_set - turkey_set
    
    results = {
        'all_three': all_three,
        'china_turkey': china_turkey,
        'china_india': china_india,
        'turkey_india': turkey_india,
        'china_only': china_only,
        'turkey_only': turkey_only,
        'india_only': india_only,
        'statistics': {
            'total_china': len(china_set),
            'total_turkey': len(turkey_set),
            'total_india': len(india_set),
            'all_three_count': len(all_three),
            'china_turkey_count': len(china_turkey),
            'china_india_count': len(china_india),
            'turkey_india_count': len(turkey_india),
            'china_only_count': len(china_only),
            'turkey_only_count': len(turkey_only),
            'india_only_count': len(india_only),
        }
    }
    
    return results


def normalize_name(name: str) -> str:
    """
    Normalize commodity name for comparison.
    
    Normalizes by:
    - Converting to lowercase
    - Removing punctuation and special characters
    - Normalizing whitespace
    - Sorting words alphabetically (to handle word order differences)
    
    Args:
        name: Original commodity name
        
    Returns:
        Normalized name for comparison
    """
    if not name:
        return ''
    
    # Convert to lowercase
    normalized = name.lower()
    
    # Remove punctuation and special characters (keep spaces and alphanumeric)
    normalized = re.sub(r'[^\w\s]', ' ', normalized)
    
    # Normalize whitespace (multiple spaces to single space)
    normalized = re.sub(r'\s+', ' ', normalized)
    
    # Strip leading/trailing spaces
    normalized = normalized.strip()
    
    # Sort words alphabetically to handle word order differences
    # e.g., "Frozen octopus" and "Octopus frozen" become the same
    words = sorted(normalized.split())
    normalized = ' '.join(words)
    
    return normalized


def compare_names_for_common_codes(china_codes: dict, turkey_codes: dict, india_codes: dict, 
                                   common_codes: set) -> list:
    """
    Compare names for codes that appear in multiple countries.
    
    Uses normalized comparison to account for:
    - Case differences (UPPERCASE vs lowercase)
    - Word order differences ("Frozen octopus" vs "Octopus frozen")
    - Punctuation differences
    
    Args:
        china_codes: Dictionary of China codes
        turkey_codes: Dictionary of Turkey codes
        india_codes: Dictionary of India codes
        common_codes: Set of codes that appear in multiple countries
        
    Returns:
        List of dictionaries with comparison details
    """
    comparisons = []
    
    for code in sorted(common_codes):
        china_name = china_codes.get(code, '')
        turkey_name = turkey_codes.get(code, '')
        india_name = india_codes.get(code, '')
        
        # Collect original names
        names = []
        if china_name:
            names.append(('China', china_name))
        if turkey_name:
            names.append(('Turkey', turkey_name))
        if india_name:
            names.append(('India', india_name))
        
        # Normalize names for comparison
        normalized_names = set()
        for _, name in names:
            if name:
                normalized = normalize_name(name)
                if normalized:  # Only add non-empty normalized names
                    normalized_names.add(normalized)
        
        # Names are considered identical if normalized versions are the same
        names_identical = len(normalized_names) <= 1
        
        comparisons.append({
            'code': code,
            'china_name': china_name,
            'turkey_name': turkey_name,
            'india_name': india_name,
            'names_identical': names_identical,
            'unique_names_count': len(normalized_names),
            'countries': [country for country, _ in names],
            'normalized_names': sorted(normalized_names)  # For debugging
        })
    
    return comparisons


def print_report(china_codes: dict, turkey_codes: dict, india_codes: dict, 
                 comparison_results: dict, output_file: Path = None):
    """
    Print detailed comparison report.
    
    Args:
        china_codes: Dictionary of China codes
        turkey_codes: Dictionary of Turkey codes
        india_codes: Dictionary of India codes
        comparison_results: Results from compare_codes
        output_file: Optional path to save report
    """
    stats = comparison_results['statistics']
    
    report_lines = []
    report_lines.append("=" * 80)
    report_lines.append("COMPARISON OF MISSING TNVED CODES ACROSS COUNTRIES")
    report_lines.append("=" * 80)
    report_lines.append("")
    
    # Overall statistics
    report_lines.append("OVERALL STATISTICS:")
    report_lines.append(f"  China missing codes:  {stats['total_china']:,}")
    report_lines.append(f"  Turkey missing codes: {stats['total_turkey']:,}")
    report_lines.append(f"  India missing codes:  {stats['total_india']:,}")
    report_lines.append("")
    
    # Code overlap statistics
    report_lines.append("CODE OVERLAP STATISTICS:")
    report_lines.append(f"  Codes missing in ALL THREE countries:     {stats['all_three_count']:,}")
    report_lines.append(f"  Codes missing in China & Turkey only:       {stats['china_turkey_count']:,}")
    report_lines.append(f"  Codes missing in China & India only:        {stats['china_india_count']:,}")
    report_lines.append(f"  Codes missing in Turkey & India only:       {stats['turkey_india_count']:,}")
    report_lines.append(f"  Codes missing ONLY in China:                 {stats['china_only_count']:,}")
    report_lines.append(f"  Codes missing ONLY in Turkey:               {stats['turkey_only_count']:,}")
    report_lines.append(f"  Codes missing ONLY in India:                {stats['india_only_count']:,}")
    report_lines.append("")
    
    # Analyze names for common codes
    report_lines.append("=" * 80)
    report_lines.append("NAME COMPARISON FOR COMMON CODES")
    report_lines.append("=" * 80)
    report_lines.append("")
    
    # Codes in all three countries
    if comparison_results['all_three']:
        report_lines.append(f"CODES MISSING IN ALL THREE COUNTRIES ({len(comparison_results['all_three']):,} codes):")
        name_comparisons = compare_names_for_common_codes(
            china_codes, turkey_codes, india_codes, comparison_results['all_three']
        )
        
        identical_names = [c for c in name_comparisons if c['names_identical']]
        different_names = [c for c in name_comparisons if not c['names_identical']]
        
        report_lines.append(f"  Codes with semantically identical names (same meaning, different format): {len(identical_names):,}")
        report_lines.append(f"  Codes with meaningfully different names: {len(different_names):,}")
        report_lines.append("")
        report_lines.append("  Note: Names are normalized (lowercase, sorted words) for comparison.")
        report_lines.append("        'Identical' means same meaning despite format differences (e.g., 'Frozen octopus' = 'OCTOPUS FROZEN').")
        report_lines.append("")
        
        if different_names:
            report_lines.append("  Examples of codes with MEANINGFULLY DIFFERENT names (first 20):")
            for comp in different_names[:20]:
                report_lines.append(f"    Code: {comp['code']}")
                if comp['china_name']:
                    report_lines.append(f"      China:  {comp['china_name'][:80]}")
                if comp['turkey_name']:
                    report_lines.append(f"      Turkey: {comp['turkey_name'][:80]}")
                if comp['india_name']:
                    report_lines.append(f"      India:  {comp['india_name'][:80]}")
                # Show normalized versions for comparison
                if len(comp['normalized_names']) > 1:
                    report_lines.append(f"      Normalized: {', '.join(comp['normalized_names'][:3])}")
                report_lines.append("")
        
        if identical_names:
            report_lines.append("  Examples of codes with SEMANTICALLY IDENTICAL names (first 10):")
            for comp in identical_names[:10]:
                name = comp['china_name'] or comp['turkey_name'] or comp['india_name']
                countries = ', '.join(comp['countries'])
                normalized = comp['normalized_names'][0] if comp['normalized_names'] else ''
                report_lines.append(f"    {comp['code']}: {name[:60]} ({countries})")
                if len(comp['countries']) > 1:
                    report_lines.append(f"      Normalized: {normalized}")
            report_lines.append("")
    
    # Codes in two countries
    for pair_name, codes_set, count_key in [
        ('China & Turkey', comparison_results['china_turkey'], 'china_turkey_count'),
        ('China & India', comparison_results['china_india'], 'china_india_count'),
        ('Turkey & India', comparison_results['turkey_india'], 'turkey_india_count'),
    ]:
        if codes_set:
            report_lines.append(f"CODES MISSING IN {pair_name} ({stats[count_key]:,} codes):")
            name_comparisons = compare_names_for_common_codes(
                china_codes, turkey_codes, india_codes, codes_set
            )
            
            identical_names = [c for c in name_comparisons if c['names_identical']]
            different_names = [c for c in name_comparisons if not c['names_identical']]
            
            report_lines.append(f"  Codes with semantically identical names: {len(identical_names):,}")
            report_lines.append(f"  Codes with meaningfully different names: {len(different_names):,}")
            
            if different_names:
                report_lines.append("  Examples of codes with MEANINGFULLY DIFFERENT names (first 10):")
                for comp in different_names[:10]:
                    report_lines.append(f"    Code: {comp['code']}")
                    if 'China' in comp['countries'] and comp['china_name']:
                        report_lines.append(f"      China:  {comp['china_name'][:80]}")
                    if 'Turkey' in comp['countries'] and comp['turkey_name']:
                        report_lines.append(f"      Turkey: {comp['turkey_name'][:80]}")
                    if 'India' in comp['countries'] and comp['india_name']:
                        report_lines.append(f"      India:  {comp['india_name'][:80]}")
                    if len(comp['normalized_names']) > 1:
                        report_lines.append(f"      Normalized: {', '.join(comp['normalized_names'][:2])}")
                    report_lines.append("")
            report_lines.append("")
    
    # Sample unique codes
    report_lines.append("=" * 80)
    report_lines.append("SAMPLE OF UNIQUE CODES (codes missing only in one country)")
    report_lines.append("=" * 80)
    report_lines.append("")
    
    for country_name, codes_set, count_key in [
        ('China', comparison_results['china_only'], 'china_only_count'),
        ('Turkey', comparison_results['turkey_only'], 'turkey_only_count'),
        ('India', comparison_results['india_only'], 'india_only_count'),
    ]:
        if codes_set:
            report_lines.append(f"{country_name} ONLY ({stats[count_key]:,} codes):")
            sample_codes = sorted(list(codes_set))[:20]
            report_lines.append(f"  Sample codes: {', '.join(sample_codes)}")
            report_lines.append("")
    
    report_text = "\n".join(report_lines)
    
    # Save to file if specified
    if output_file:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(report_text)
        logger.info(f"Report saved to {output_file}")
    
    # Print to console (try with UTF-8 encoding)
    try:
        print(report_text)
    except UnicodeEncodeError:
        # If console can't handle UTF-8, just print summary
        logger.info("Report generated. See file for full details.")
        print(f"\nSummary:")
        print(f"  China: {stats['total_china']:,} codes")
        print(f"  Turkey: {stats['total_turkey']:,} codes")
        print(f"  India: {stats['total_india']:,} codes")
        print(f"  Common to all three: {stats['all_three_count']:,} codes")


def save_comparison_csv(china_codes: dict, turkey_codes: dict, india_codes: dict,
                       comparison_results: dict, output_csv: Path):
    """
    Save detailed comparison to CSV.
    
    Args:
        china_codes: Dictionary of China codes
        turkey_codes: Dictionary of Turkey codes
        india_codes: Dictionary of India codes
        comparison_results: Results from compare_codes
        output_csv: Path to output CSV file
    """
    logger.info(f"Saving comparison to {output_csv}")
    
    all_codes = set(china_codes.keys()) | set(turkey_codes.keys()) | set(india_codes.keys())
    
    comparison_data = []
    for code in sorted(all_codes):
        in_china = code in china_codes
        in_turkey = code in turkey_codes
        in_india = code in india_codes
        
        comparison_data.append({
            'TNVED': code,
            'In_China': in_china,
            'In_Turkey': in_turkey,
            'In_India': in_india,
            'China_Name': china_codes.get(code, ''),
            'Turkey_Name': turkey_codes.get(code, ''),
            'India_Name': india_codes.get(code, ''),
            'Category': (
                'All_Three' if (in_china and in_turkey and in_india) else
                'China_Turkey' if (in_china and in_turkey) else
                'China_India' if (in_china and in_india) else
                'Turkey_India' if (in_turkey and in_india) else
                'China_Only' if in_china else
                'Turkey_Only' if in_turkey else
                'India_Only' if in_india else 'Unknown'
            )
        })
    
    df = pd.DataFrame(comparison_data)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False, encoding='utf-8-sig')
    logger.info(f"Saved {len(comparison_data)} codes to CSV")


def main():
    """Main function."""
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    
    reports_dir = project_root / 'reports'
    
    china_csv = reports_dir / 'china_tnved_missing_codes.csv'
    turkey_csv = reports_dir / 'turkey_tnved_missing_codes.csv'
    india_csv = reports_dir / 'india_tnved_missing_codes.csv'
    
    logger.info("Loading missing codes from all countries...")
    
    # Load codes
    china_codes = load_missing_codes('China', china_csv)
    turkey_codes = load_missing_codes('Turkey', turkey_csv)
    india_codes = load_missing_codes('India', india_csv)
    
    if not china_codes and not turkey_codes and not india_codes:
        logger.error("No codes loaded from any country. Exiting.")
        return
    
    # Compare codes
    comparison_results = compare_codes(china_codes, turkey_codes, india_codes)
    
    # Print report
    output_report = reports_dir / 'missing_codes_comparison.txt'
    print_report(china_codes, turkey_codes, india_codes, comparison_results, output_report)
    
    # Save detailed CSV
    output_csv = reports_dir / 'missing_codes_comparison.csv'
    save_comparison_csv(china_codes, turkey_codes, india_codes, comparison_results, output_csv)
    
    logger.info("Comparison completed!")


if __name__ == "__main__":
    main()

