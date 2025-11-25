#!/usr/bin/env python3
"""
Script to translate missing TNVED codes to Russian using ChatGPT API.

This script:
1. Loads missing codes from China, Turkey, and India
2. Filters out codes that already have Russian names in reference
3. Uses ChatGPT API to translate commodity names to Russian
4. Saves translations to a mapping file
"""

import pandas as pd
from pathlib import Path
import logging
import json
import os
import time
from openai import OpenAI
from collections import defaultdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_russian_tnved_names(tnved_file: Path) -> dict:
    """
    Load Russian TNVED names from reference file.
    
    Args:
        tnved_file: Path to metadata/tnved.csv
        
    Returns:
        Dictionary mapping code -> Russian name
    """
    logger.info(f"Loading Russian TNVED names from {tnved_file}")
    
    if not tnved_file.exists():
        logger.error(f"File does not exist: {tnved_file}")
        return {}
    
    try:
        df = pd.read_csv(tnved_file, dtype={'KOD': str, 'NAME': str, 'level': int})
        df.columns = df.columns.str.upper()
        
        code_names = {}
        for _, row in df.iterrows():
            kod = str(row['KOD']).strip()
            level = int(row['LEVEL'])
            name = str(row['NAME']).strip()
            
            # Normalize code based on level
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
                continue
            
            # Store name (prefer level 10)
            if level == 10:
                code_names[kod_normalized] = name
            elif kod_normalized not in code_names:
                code_names[kod_normalized] = name
        
        logger.info(f"Loaded {len(code_names)} Russian TNVED names")
        return code_names
        
    except Exception as e:
        logger.error(f"Failed to load Russian TNVED names: {e}")
        return {}


def load_missing_codes_with_names(reports_dir: Path) -> dict:
    """
    Load missing codes with their original names from all countries.
    
    Args:
        reports_dir: Path to reports directory
        
    Returns:
        Dictionary mapping code -> list of (country, name) tuples
    """
    logger.info("Loading missing codes with names from all countries...")
    
    code_names = defaultdict(list)
    
    for country, filename in [
        ('China', 'china_tnved_missing_codes.csv'),
        ('Turkey', 'turkey_tnved_missing_codes.csv'),
        ('India', 'india_tnved_missing_codes.csv'),
    ]:
        csv_file = reports_dir / filename
        if not csv_file.exists():
            logger.warning(f"File not found: {csv_file}")
            continue
        
        try:
            df = pd.read_csv(csv_file, dtype={'TNVED': str})
            for _, row in df.iterrows():
                code = str(row['TNVED']).strip()
                name = row.get('HS_NAME', '')
                if pd.notna(name) and str(name).strip():
                    code_names[code].append((country, str(name).strip()))
            
            logger.info(f"  Loaded {len(df)} codes from {country}")
        except Exception as e:
            logger.error(f"Failed to load {country} codes: {e}")
    
    logger.info(f"Total unique codes with names: {len(code_names)}")
    return dict(code_names)


def get_best_name_for_translation(code: str, names_by_country: list) -> str:
    """
    Select the best name for translation from multiple country sources.
    
    Args:
        code: TNVED code
        names_by_country: List of (country, name) tuples
        
    Returns:
        Best name to use for translation
    """
    if not names_by_country:
        return ''
    
    # Prefer longer, more descriptive names
    # Sort by length (descending) and take the longest
    sorted_names = sorted(names_by_country, key=lambda x: len(x[1]), reverse=True)
    return sorted_names[0][1]


def translate_with_chatgpt(client: OpenAI, text: str, max_retries: int = 3) -> str:
    """
    Translate text to Russian using ChatGPT API.
    
    Args:
        client: OpenAI client
        text: Text to translate
        max_retries: Maximum number of retry attempts
        
    Returns:
        Translated text in Russian
    """
    if not text or not text.strip():
        return ''
    
    prompt = f"""Translate the following official HS (Harmonized System) commodity code name to Russian.

This is an official trade classification name used in international customs and trade statistics. 
Use official Russian terminology consistent with the Russian TNVED classification system.
Maintain the formal, technical style typical of customs nomenclature.

Return only the Russian translation, without any explanations or additional text.
If the text is already in Russian, return it as is.

Official HS commodity name to translate: {text}

Official Russian translation:"""
    
    system_prompt = """You are a professional translator specializing in international trade classification systems (HS/TNVED). 
You translate official commodity names from the Harmonized System to Russian, using official terminology consistent with 
Russian customs nomenclature. Your translations are formal, technical, and accurate, matching the style of official 
customs classification documents."""
    
    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",  # Using cheaper model for translation
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,  # Lower temperature for more consistent, official translations
                max_tokens=200
            )
            
            translated = response.choices[0].message.content.strip()
            
            # Remove quotes if present
            if translated.startswith('"') and translated.endswith('"'):
                translated = translated[1:-1]
            if translated.startswith("'") and translated.endswith("'"):
                translated = translated[1:-1]
            
            return translated
            
        except Exception as e:
            logger.warning(f"Translation attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                logger.error(f"Failed to translate after {max_retries} attempts: {text}")
                return ''
    
    return ''


def translate_missing_codes(
    missing_codes: dict,
    russian_names: dict,
    api_key: str,
    output_file: Path,
    resume_file: Path = None
) -> dict:
    """
    Translate missing codes to Russian.
    
    Args:
        missing_codes: Dictionary mapping code -> list of (country, name) tuples
        russian_names: Dictionary of existing Russian names
        api_key: OpenAI API key
        output_file: Path to save translations
        resume_file: Path to resume file (for continuing after interruption)
        
    Returns:
        Dictionary mapping code -> Russian translation
    """
    logger.info("Starting translation process...")
    
    # Initialize OpenAI client
    client = OpenAI(api_key=api_key)
    
    # Load existing translations if resuming
    translations = {}
    if resume_file and resume_file.exists():
        try:
            with open(resume_file, 'r', encoding='utf-8') as f:
                translations = json.load(f)
            logger.info(f"Loaded {len(translations)} existing translations from {resume_file}")
        except Exception as e:
            logger.warning(f"Failed to load resume file: {e}")
    
    # Filter codes that need translation
    codes_to_translate = []
    for code, names_list in missing_codes.items():
        # Skip if already has Russian name
        if code in russian_names:
            continue
        
        # Skip if already translated
        if code in translations:
            continue
        
        best_name = get_best_name_for_translation(code, names_list)
        if best_name:
            codes_to_translate.append((code, best_name))
    
    logger.info(f"Codes to translate: {len(codes_to_translate):,}")
    logger.info(f"Codes already in Russian reference: {sum(1 for c in missing_codes.keys() if c in russian_names):,}")
    logger.info(f"Codes already translated: {len(translations):,}")
    
    # Translate codes
    total = len(codes_to_translate)
    for idx, (code, name) in enumerate(codes_to_translate, 1):
        if idx % 10 == 0:
            logger.info(f"Progress: {idx}/{total} ({idx/total*100:.1f}%)")
            # Save progress
            if resume_file:
                with open(resume_file, 'w', encoding='utf-8') as f:
                    json.dump(translations, f, ensure_ascii=False, indent=2)
        
        translated = translate_with_chatgpt(client, name)
        if translated:
            translations[code] = {
                'original_name': name,
                'russian_name': translated,
                'source_countries': [country for country, _ in missing_codes[code]]
            }
        else:
            logger.warning(f"Failed to translate code {code}: {name}")
        
        # Rate limiting - small delay to avoid hitting rate limits
        time.sleep(0.5)
    
    # Save final translations
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(translations, f, ensure_ascii=False, indent=2)
    
    logger.info(f"Saved {len(translations)} translations to {output_file}")
    
    return translations


def save_translations_csv(translations: dict, missing_codes: dict, output_csv: Path):
    """
    Save translations to CSV file for easy review.
    
    Args:
        translations: Dictionary of translations
        missing_codes: Original missing codes dictionary
        output_csv: Path to output CSV file
    """
    logger.info(f"Saving translations to CSV: {output_csv}")
    
    rows = []
    for code, trans_data in translations.items():
        original_name = trans_data.get('original_name', '')
        russian_name = trans_data.get('russian_name', '')
        source_countries = ', '.join(trans_data.get('source_countries', []))
        
        rows.append({
            'TNVED': code,
            'Original_Name': original_name,
            'Russian_Name': russian_name,
            'Source_Countries': source_countries,
            'TNVED2': code[:2],
            'TNVED4': code[:4],
            'TNVED6': code[:6],
            'TNVED8': code[:8],
        })
    
    df = pd.DataFrame(rows)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False, encoding='utf-8-sig')
    logger.info(f"Saved {len(rows)} translations to CSV")


def main():
    """Main function."""
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    
    # Paths
    reports_dir = project_root / 'reports'
    metadata_dir = project_root / 'metadata'
    output_dir = project_root / 'metadata' / 'translations'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Get API key from environment variable
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        logger.error("OPENAI_API_KEY environment variable not set!")
        logger.info("Please set it with: export OPENAI_API_KEY='your-key-here'")
        return
    
    # Load Russian names
    tnved_file = metadata_dir / 'tnved.csv'
    russian_names = load_russian_tnved_names(tnved_file)
    
    # Load missing codes
    missing_codes = load_missing_codes_with_names(reports_dir)
    
    # Output files
    output_json = output_dir / 'missing_codes_translations.json'
    output_csv = output_dir / 'missing_codes_translations.csv'
    resume_file = output_dir / 'translations_resume.json'
    
    # Translate
    translations = translate_missing_codes(
        missing_codes,
        russian_names,
        api_key,
        output_json,
        resume_file
    )
    
    # Save CSV
    if translations:
        save_translations_csv(translations, missing_codes, output_csv)
    
    logger.info("Translation process completed!")
    logger.info(f"Results saved to:")
    logger.info(f"  JSON: {output_json}")
    logger.info(f"  CSV: {output_csv}")


if __name__ == "__main__":
    main()

