#!/usr/bin/env python3
"""
Script to merge processed parquet files (data_processed/) and save to DuckDB.

This script:
1. Reads processed parquet files from data_processed/ folder
2. Validates each dataset against the data model schema
3. Optionally, loads and transforms Comtrade data for missing countries
4. Merges all datasets into one unified dataset
5. Excludes countries specified in --exclude-countries argument
6. start_year argument to filter data from this year onwards
7. Saves the merged dataset to DuckDB format in db/unified_trade_data.duckdb
"""

import pandas as pd
import duckdb
from pathlib import Path
import logging
import argparse
import json
from typing import Dict, List
import numpy as np
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define the expected schema from data_model.md
EXPECTED_SCHEMA = {
    'NAPR': 'object',          # VARCHAR - торговый поток (ИМ/ЭК)
    'PERIOD': 'datetime64[ns]', # DATE - отчетный период
    'STRANA': 'object',         # VARCHAR - страна-отчет (ISO код)
    'TNVED': 'object',          # VARCHAR - код ТН ВЭД (8-10 знаков)
    'EDIZM': 'object',          # VARCHAR - единица измерения
    'EDIZM_ISO': 'object',      # VARCHAR - ISO код единицы измерения (опционально)
    'STOIM': 'float64',         # DECIMAL - стоимость в ТЫСЯЧАХ USD
    'NETTO': 'float64',         # DECIMAL - вес нетто в кг
    'KOL': 'float64',           # DECIMAL - количество в дополнительной единице
    'TNVED4': 'object',         # VARCHAR - первые 4 знака TNVED
    'TNVED6': 'object',         # VARCHAR - первые 6 знаков TNVED
    'TNVED2': 'object'          # VARCHAR - первые 2 знака TNVED
}

#--------------------------------
# Функции для выделения выбросов:
#--------------------------------

def show_outliers(x: pd.Series, nsd: float, tv: float) -> int:
    """
    Подсчитывает количество выбросов в ряде данных.
    
    Выбросом считается значение, где:
    - |z-score| > nsd (число стандартных отклонений)
    - значение > tv (пороговое значение)
    
    Args:
        x: Ряд данных для анализа
        nsd: Количество стандартных отклонений для определения выброса
        tv: Пороговое значение (выброс должен быть больше этого значения)
        
    Returns:
        Количество выбросов
    """
    if x.empty or x.isna().all():
        return 0
    
    mean_val = x.mean(skipna=True)
    std_val = x.std(skipna=True)
    
    if std_val == 0 or pd.isna(std_val):
        return 0
    
    z = (x - mean_val) / std_val
    outliers = ((z.abs() > nsd) & (x > tv)).sum()
    
    return int(outliers)


def outlier_frac(x: pd.Series, y: pd.Series, nsd: float, tv: float) -> int:
    """
    Подсчитывает количество выбросов в отношении x/y.
    
    Выбросом считается значение, где:
    - |z-score отношения x/y| > nsd
    - x > tv (пороговое значение)
    
    Args:
        x: Числитель (первый ряд данных)
        y: Знаменатель (второй ряд данных)
        nsd: Количество стандартных отклонений для определения выброса
        tv: Пороговое значение для x
        
    Returns:
        Количество выбросов
    """
    if x.empty or y.empty or x.isna().all() or y.isna().all():
        return 0
    
    # Вычисляем отношение x/y, избегая деления на ноль
    ratio = x / y.replace(0, np.nan)
    
    if ratio.isna().all():
        return 0
    
    mean_ratio = ratio.mean(skipna=True)
    std_ratio = ratio.std(skipna=True)
    
    if std_ratio == 0 or pd.isna(std_ratio):
        return 0
    
    z_sc = (ratio - mean_ratio) / std_ratio
    outliers = ((z_sc.abs() > nsd) & (x > tv)).sum()
    
    return int(outliers)


def detect_outliers_in_dataframe(
    df: pd.DataFrame,
    nsd: float = 3.0,
    tv_kol: float = 0.0
) -> Dict[str, int]:
    """
    Обнаруживает выбросы в колонке KOL (количество в дополнительной единице).
    
    Args:
        df: DataFrame для анализа
        nsd: Количество стандартных отклонений для определения выброса (по умолчанию 3.0)
        tv_kol: Пороговое значение для KOL (по умолчанию 0.0)
        
    Returns:
        Словарь с количеством выбросов в KOL
    """
    results = {}
    
    # Выбросы в KOL
    if 'KOL' in df.columns:
        results['kol_outliers'] = show_outliers(df['KOL'], nsd, tv_kol)
    else:
        results['kol_outliers'] = 0
    
    return results


def detect_outliers_by_time_series(
    df: pd.DataFrame,
    nsd: float = 6.0,
    tv: float = 1e6,
    require_all_methods: bool = True
) -> pd.DataFrame:
    """
    Обнаруживает выбросы в KOL, группируя данные по временным рядам (STRANA, TNVED, NAPR).
    
    Использует три метода обнаружения выбросов:
    1. show_outliers(KOL) - выбросы в абсолютных значениях KOL
    2. outlier_frac(KOL, STOIM) - выбросы в отношении KOL/STOIM
    3. outlier_frac(KOL, NETTO) - выбросы в отношении KOL/NETTO
    
    Args:
        df: DataFrame для анализа (должен содержать STRANA, TNVED, NAPR, KOL, STOIM, NETTO, PERIOD)
        nsd: Количество стандартных отклонений для определения выброса (по умолчанию 6.0, как в outlier_detection.Rmd)
        tv: Пороговое значение для KOL (по умолчанию 10^6, как в outlier_detection.Rmd)
        require_all_methods: Если True, возвращает только ряды, где все три метода нашли выбросы
        
    Returns:
        DataFrame с колонками: STRANA, TNVED, NAPR, outliers_1, outliers_2, outliers_3
        где outliers_1 - выбросы в KOL, outliers_2 - в KOL/STOIM, outliers_3 - в KOL/NETTO
    """
    required_cols = ['STRANA', 'TNVED', 'NAPR', 'KOL', 'PERIOD']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        logger.warning(f"Cannot detect outliers by time series: missing columns {missing_cols}")
        return pd.DataFrame()
    
    # Сортируем по периоду для правильной группировки временных рядов
    df_sorted = df.sort_values('PERIOD').copy()
    
    # Группируем по временным рядам
    results = []
    
    for (strana, tnved, napr), group in df_sorted.groupby(['STRANA', 'TNVED', 'NAPR']):
        if group.empty:
            continue
        
        # Метод 1: выбросы в KOL
        kol_series = group['KOL'].dropna()
        outliers_1 = show_outliers(kol_series, nsd, tv) if not kol_series.empty else 0
        
        # Метод 2: выбросы в KOL/STOIM
        outliers_2 = 0
        if 'STOIM' in group.columns:
            valid_mask = group['STOIM'].notna() & group['KOL'].notna() & (group['STOIM'] != 0)
            if valid_mask.sum() > 0:
                outliers_2 = outlier_frac(
                    group.loc[valid_mask, 'KOL'],
                    group.loc[valid_mask, 'STOIM'],
                    nsd,
                    tv
                )
        
        # Метод 3: выбросы в KOL/NETTO
        outliers_3 = 0
        if 'NETTO' in group.columns:
            valid_mask = group['NETTO'].notna() & group['KOL'].notna() & (group['NETTO'] != 0)
            if valid_mask.sum() > 0:
                outliers_3 = outlier_frac(
                    group.loc[valid_mask, 'KOL'],
                    group.loc[valid_mask, 'NETTO'],
                    nsd,
                    tv
                )
        
        # Если require_all_methods=True, пропускаем ряды где не все методы нашли выбросы
        if require_all_methods:
            if not (outliers_1 >= 1 and outliers_2 >= 1 and outliers_3 >= 1):
                continue
        
        results.append({
            'STRANA': strana,
            'TNVED': tnved,
            'NAPR': napr,
            'outliers_1': outliers_1,  # KOL
            'outliers_2': outliers_2,    # KOL/STOIM
            'outliers_3': outliers_3     # KOL/NETTO
        })
    
    if not results:
        return pd.DataFrame(columns=['STRANA', 'TNVED', 'NAPR', 'outliers_1', 'outliers_2', 'outliers_3'])
    
    return pd.DataFrame(results)


def replace_outliers_with_nan(
    df: pd.DataFrame,
    outlier_series: pd.DataFrame,
    nsd: float = 6.0,
    tv: float = 1e6
) -> pd.DataFrame:
    """
    Заменяет выбросы в KOL на NaN для временных рядов, где все три метода обнаружили выбросы.
    
    Args:
        df: DataFrame с данными
        outlier_series: DataFrame с результатами detect_outliers_by_time_series
        nsd: Количество стандартных отклонений (должно совпадать с параметром обнаружения)
        tv: Пороговое значение (должно совпадать с параметром обнаружения)
        
    Returns:
        DataFrame с замененными выбросами
    """
    if outlier_series.empty:
        logger.info("No outlier series to process, skipping replacement")
        return df
    
    df_result = df.copy()
    replaced_count = 0
    
    # Для каждого временного ряда с выбросами
    for _, outlier_row in outlier_series.iterrows():
        strana = outlier_row['STRANA']
        tnved = outlier_row['TNVED']
        napr = outlier_row['NAPR']
        
        # Находим соответствующие строки в основном DataFrame
        mask = (
            (df_result['STRANA'] == strana) &
            (df_result['TNVED'] == tnved) &
            (df_result['NAPR'] == napr) &
            (df_result['KOL'].notna())
        )
        
        if not mask.any():
            continue
        
        # Получаем группу с исходными индексами
        group_indices = df_result.index[mask]
        group = df_result.loc[group_indices]
        
        # Метод 1: выбросы в KOL
        kol_values = group['KOL'].dropna()
        outlier_mask_1 = pd.Series(False, index=group_indices)
        if not kol_values.empty:
            mean_kol = kol_values.mean()
            std_kol = kol_values.std()
            if std_kol > 0:
                z_kol = (group['KOL'] - mean_kol) / std_kol
                outlier_mask_1 = (z_kol.abs() > nsd) & (group['KOL'] > tv)
        
        # Метод 2: выбросы в KOL/STOIM
        outlier_mask_2 = pd.Series(False, index=group_indices)
        if 'STOIM' in group.columns:
            valid_mask = group['STOIM'].notna() & (group['STOIM'] != 0) & group['KOL'].notna()
            if valid_mask.any():
                valid_indices = group_indices[valid_mask]
                ratio = group.loc[valid_indices, 'KOL'] / group.loc[valid_indices, 'STOIM']
                mean_ratio = ratio.mean()
                std_ratio = ratio.std()
                if std_ratio > 0:
                    z_ratio = (ratio - mean_ratio) / std_ratio
                    outlier_mask_2.loc[valid_indices] = (z_ratio.abs() > nsd) & (group.loc[valid_indices, 'KOL'] > tv)
        
        # Метод 3: выбросы в KOL/NETTO
        outlier_mask_3 = pd.Series(False, index=group_indices)
        if 'NETTO' in group.columns:
            valid_mask = group['NETTO'].notna() & (group['NETTO'] != 0) & group['KOL'].notna()
            if valid_mask.any():
                valid_indices = group_indices[valid_mask]
                ratio = group.loc[valid_indices, 'KOL'] / group.loc[valid_indices, 'NETTO']
                mean_ratio = ratio.mean()
                std_ratio = ratio.std()
                if std_ratio > 0:
                    z_ratio = (ratio - mean_ratio) / std_ratio
                    outlier_mask_3.loc[valid_indices] = (z_ratio.abs() > nsd) & (group.loc[valid_indices, 'KOL'] > tv)
        
        # Заменяем значения, где хотя бы один метод обнаружил выброс
        final_outlier_mask = outlier_mask_1 | outlier_mask_2 | outlier_mask_3
        
        if final_outlier_mask.any():
            outlier_indices = group_indices[final_outlier_mask]
            df_result.loc[outlier_indices, 'KOL'] = np.nan
            replaced_count += final_outlier_mask.sum()
    
    return df_result


def create_outlier_report(
    df: pd.DataFrame,
    outlier_series: pd.DataFrame,
    replaced_count: int = 0,
    keep_outliers: bool = False,
    nsd: float = 6.0,
    tv: float = 1e6
) -> pd.DataFrame:
    """
    Создает детальный отчет о выбросах с информацией о конкретных значениях.
    
    Args:
        df: Исходный DataFrame (до замены выбросов, если была выполнена)
        outlier_series: DataFrame с результатами detect_outliers_by_time_series
        replaced_count: Количество замененных выбросов
        keep_outliers: Были ли выбросы оставлены как есть
        nsd: Количество стандартных отклонений
        tv: Пороговое значение
        
    Returns:
        DataFrame с детальным отчетом о выбросах
    """
    if outlier_series.empty:
        return pd.DataFrame()
    
    report_rows = []
    
    # Для каждого временного ряда с выбросами
    for _, outlier_row in outlier_series.iterrows():
        strana = outlier_row['STRANA']
        tnved = outlier_row['TNVED']
        napr = outlier_row['NAPR']
        
        # Находим соответствующие строки
        mask = (
            (df['STRANA'] == strana) &
            (df['TNVED'] == tnved) &
            (df['NAPR'] == napr) &
            (df['KOL'].notna())
        )
        
        if not mask.any():
            continue
        
        group = df.loc[mask].copy()
        
        # Вычисляем статистику для определения выбросов
        kol_values = group['KOL'].dropna()
        if kol_values.empty:
            continue
        
        mean_kol = kol_values.mean()
        std_kol = kol_values.std()
        
        # Метод 1: выбросы в KOL
        z_kol = None
        if std_kol > 0:
            z_kol = (group['KOL'] - mean_kol) / std_kol
            outlier_mask_1 = (z_kol.abs() > nsd) & (group['KOL'] > tv)
        else:
            outlier_mask_1 = pd.Series(False, index=group.index)
        
        # Метод 2: выбросы в KOL/STOIM
        outlier_mask_2 = pd.Series(False, index=group.index)
        if 'STOIM' in group.columns:
            valid_mask = group['STOIM'].notna() & (group['STOIM'] != 0) & group['KOL'].notna()
            if valid_mask.any():
                ratio = group.loc[valid_mask, 'KOL'] / group.loc[valid_mask, 'STOIM']
                mean_ratio = ratio.mean()
                std_ratio = ratio.std()
                if std_ratio > 0:
                    z_ratio = (ratio - mean_ratio) / std_ratio
                    outlier_mask_2.loc[valid_mask] = (z_ratio.abs() > nsd) & (group.loc[valid_mask, 'KOL'] > tv)
        
        # Метод 3: выбросы в KOL/NETTO
        outlier_mask_3 = pd.Series(False, index=group.index)
        if 'NETTO' in group.columns:
            valid_mask = group['NETTO'].notna() & (group['NETTO'] != 0) & group['KOL'].notna()
            if valid_mask.any():
                ratio = group.loc[valid_mask, 'KOL'] / group.loc[valid_mask, 'NETTO']
                mean_ratio = ratio.mean()
                std_ratio = ratio.std()
                if std_ratio > 0:
                    z_ratio = (ratio - mean_ratio) / std_ratio
                    outlier_mask_3.loc[valid_mask] = (z_ratio.abs() > nsd) & (group.loc[valid_mask, 'KOL'] > tv)
        
        # Находим все выбросы
        final_outlier_mask = outlier_mask_1 | outlier_mask_2 | outlier_mask_3
        
        if final_outlier_mask.any():
            outliers_data = group.loc[final_outlier_mask]
            
            for idx, row in outliers_data.iterrows():
                # Определяем, какими методами обнаружен выброс
                methods = []
                if outlier_mask_1.loc[idx]:
                    methods.append('KOL')
                if outlier_mask_2.loc[idx]:
                    methods.append('KOL/STOIM')
                if outlier_mask_3.loc[idx]:
                    methods.append('KOL/NETTO')
                
                report_rows.append({
                    'STRANA': strana,
                    'TNVED': tnved,
                    'NAPR': napr,
                    'PERIOD': row.get('PERIOD', ''),
                    'KOL': row.get('KOL', ''),
                    'STOIM': row.get('STOIM', ''),
                    'NETTO': row.get('NETTO', ''),
                    'KOL_Mean': mean_kol,
                    'KOL_Std': std_kol,
                    'Z_Score_KOL': z_kol.loc[idx] if z_kol is not None and idx in z_kol.index else None,
                    'Detection_Methods': ', '.join(methods),
                    'Outliers_Method1': outlier_row['outliers_1'],
                    'Outliers_Method2': outlier_row['outliers_2'],
                    'Outliers_Method3': outlier_row['outliers_3'],
                    'Total_Outliers_in_Series': outlier_row['outliers_1'] + outlier_row['outliers_2'] + outlier_row['outliers_3']
                })
    
    if not report_rows:
        return pd.DataFrame()
    
    return pd.DataFrame(report_rows)


def save_outlier_report(
    report_df: pd.DataFrame,
    outlier_summary: pd.DataFrame,
    replaced_count: int,
    keep_outliers: bool,
    output_dir: Path,
    nsd: float = 6.0,
    tv: float = 1e6
):
    """
    Сохраняет отчет о выбросах в CSV и JSON форматах.
    
    Args:
        report_df: Детальный отчет о выбросах
        outlier_summary: Сводка по временным рядам с выбросами
        replaced_count: Количество замененных выбросов
        keep_outliers: Были ли выбросы оставлены как есть
        output_dir: Директория для сохранения отчета
        nsd: Количество стандартных отклонений
        tv: Пороговое значение
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Сохраняем детальный отчет
    if not report_df.empty:
        csv_path = output_dir / f'outliers_detailed_{timestamp}.csv'
        report_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        logger.info(f"Saved detailed outlier report to {csv_path}")
    
    # Сохраняем сводку по временным рядам
    if not outlier_summary.empty:
        summary_path = output_dir / f'outliers_summary_{timestamp}.csv'
        outlier_summary.to_csv(summary_path, index=False, encoding='utf-8-sig')
        logger.info(f"Saved outlier summary to {summary_path}")
        
        # Также сохраняем JSON с метаданными
        json_path = output_dir / f'outliers_report_{timestamp}.json'
        report_metadata = {
            'timestamp': timestamp,
            'detection_parameters': {
                'nsd': nsd,
                'tv': tv,
                'require_all_methods': True
            },
            'summary': {
                'total_time_series_with_outliers': len(outlier_summary),
                'total_outliers_method1': int(outlier_summary['outliers_1'].sum()),
                'total_outliers_method2': int(outlier_summary['outliers_2'].sum()),
                'total_outliers_method3': int(outlier_summary['outliers_3'].sum()),
                'total_outliers_all_methods': int(
                    outlier_summary['outliers_1'].sum() + 
                    outlier_summary['outliers_2'].sum() + 
                    outlier_summary['outliers_3'].sum()
                )
            },
            'replacement': {
                'replaced_with_nan': replaced_count,
                'keep_outliers': keep_outliers
            },
            'detailed_records_count': len(report_df) if not report_df.empty else 0
        }
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(report_metadata, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved outlier report metadata to {json_path}")


def validate_schema(df: pd.DataFrame, filename: str) -> bool:
    """
    Validate DataFrame against expected schema.
    
    Args:
        df: DataFrame to validate
        filename: Name of the file for error reporting
        
    Returns:
        True if schema is valid, False otherwise


    """
    logger.info(f"Validating schema for {filename}")
    
    # Check if all required columns are present
    missing_cols = set(EXPECTED_SCHEMA.keys()) - set(df.columns)
    if missing_cols:
        logger.error(f"Missing columns in {filename}: {missing_cols}")
        return False
    
    # Check for extra columns
    extra_cols = set(df.columns) - set(EXPECTED_SCHEMA.keys())
    if extra_cols:
        logger.warning(f"Extra columns in {filename}: {extra_cols}")
    
    # Check data types (only for non-null values)
    for col, expected_type in EXPECTED_SCHEMA.items():
        if col in df.columns:
            actual_type = df[col].dtype
            
            # Convert period to datetime if it's not already
            if col == 'PERIOD' and actual_type != 'datetime64[ns]':
                try:
                    df[col] = pd.to_datetime(df[col])
                    actual_type = df[col].dtype
                except Exception as e:
                    logger.error(f"Failed to convert PERIOD to datetime in {filename}: {e}")
                    return False
            
            if actual_type != expected_type:
                logger.error(f"Column {col} has wrong type in {filename}: expected {expected_type}, got {actual_type}")
                return False
    
    # Validate specific values
    if 'NAPR' in df.columns:
        invalid_napr = df[~df['NAPR'].isin(['ИМ', 'ЭК'])]['NAPR'].unique()
        if len(invalid_napr) > 0:
            logger.error(f"Invalid NAPR values in {filename}: {invalid_napr}")
            return False
    
    if 'PERIOD' in df.columns:
        invalid_periods = df[df['PERIOD'].isnull()]
        if len(invalid_periods) > 0:
            logger.error(f"Null periods found in {filename}")
            return False
    
    logger.info(f"Schema validation passed for {filename}")
    return True

def load_and_validate_file(file_path: Path, start_year: int = None) -> pd.DataFrame:
    """
    Load parquet file and validate schema.
    
    Args:
        file_path: Path to parquet file
        start_year: Optional year to filter data from
        
    Returns:
        Validated DataFrame or None if validation fails
    """
    try:
        logger.info(f"Loading {file_path}")
        df = pd.read_parquet(file_path)
        
        if start_year:
            if 'PERIOD' not in df.columns:
                logger.warning(f"Cannot filter by year: {file_path.name} has no PERIOD column.")
            else:
                if df['PERIOD'].dtype != 'datetime64[ns]':
                    df['PERIOD'] = pd.to_datetime(df['PERIOD'], errors='coerce')
                
                initial_rows = len(df)
                df = df[df['PERIOD'].dt.year >= start_year].copy()
                if len(df) < initial_rows:
                    logger.info(f"Filtered {file_path.name} by start_year >= {start_year}. Kept {len(df)} of {initial_rows} rows.")

        # Validate schema
        if not validate_schema(df, file_path.name):
            logger.error(f"Schema validation failed for {file_path.name}")
            return None
        
        logger.info(f"Successfully loaded {file_path.name}: {len(df)} rows")
        return df
        
    except Exception as e:
        logger.error(f"Failed to load {file_path}: {e}")
        return None

def generate_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate derived TNVED columns if they don't exist or validate them.
    
    Args:
        df: DataFrame to process
        
    Returns:
        DataFrame with validated/generated derived columns
    """


    df_processed = df.copy()
    
    # Ensure TNVED columns are strings and generate derived columns
    if 'TNVED' in df_processed.columns:
        # Convert TNVED to string
        df_processed['TNVED'] = df_processed['TNVED'].astype(str).str.strip()
        
        # Remove leading zeros first, then pad to 10 digits on the RIGHT
        # This ensures codes like "0000870421" become "8704210000" (not "0000870421")
        # Step 1: Remove leading zeros
        df_processed['TNVED'] = df_processed['TNVED'].str.lstrip('0')
        # Step 2: Handle all-zeros case
        df_processed.loc[df_processed['TNVED'] == '', 'TNVED'] = '0'
        # Step 3: Pad to 10 digits on the RIGHT (not left!)
        def pad_right(code):
            if len(code) >= 10:
                return code[:10]
            return code + '0' * (10 - len(code))
        df_processed['TNVED'] = df_processed['TNVED'].apply(pad_right)
        
        # Generate derived columns directly from TNVED
        df_processed['TNVED2'] = df_processed['TNVED'].str[:2]
        df_processed['TNVED4'] = df_processed['TNVED'].str[:4]
        df_processed['TNVED6'] = df_processed['TNVED'].str[:6]
        df_processed['TNVED8'] = df_processed['TNVED'].str[:8]
            
    return df_processed


def save_to_duckdb(df: pd.DataFrame, output_path: Path, table_name: str = 'unified_trade_data', chunk_size: int = 100000):
    """
    Save DataFrame to DuckDB database in chunks to conserve memory.
    
    Args:
        df: DataFrame to save
        output_path: Path to DuckDB file
        table_name: Name of the table in database
        chunk_size: Number of rows to write per chunk
    """
    logger.info(f"Saving merged data to DuckDB: {output_path}")

    # It's safer to delete the old DB file to ensure a clean write.
    if output_path.exists():
        output_path.unlink()

    if df.empty:
        logger.warning("Input DataFrame is empty. Nothing to save to DuckDB.")
        return

    try:
        conn = duckdb.connect(str(output_path))
        
        # Create the table and insert the first chunk
        first_chunk = df.iloc[:chunk_size]
        conn.register('first_chunk_df', first_chunk)
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM first_chunk_df")
        conn.unregister('first_chunk_df')
        logger.info(f"  ... created table and inserted first {len(first_chunk):,} rows")

        # Insert the rest of the data in chunks using the efficient append method
        for i in range(chunk_size, len(df), chunk_size):
            chunk = df.iloc[i:i + chunk_size]
            conn.append(table_name, chunk)
            logger.info(f"  ... inserted {i + len(chunk):,} / {len(df):,} rows")

        # Get row count for verification
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        row_count = result[0]
        
        conn.close()

        if row_count != len(df):
            logger.warning(f"Row count mismatch! Expected {len(df):,}, but DuckDB table has {row_count:,}.")
        
        logger.info(f"Successfully saved {row_count:,} rows to {output_path}")
        
    except Exception as e:
        logger.error(f"Failed to save to DuckDB: {e}")
        raise

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
            for code, name in mapping.items():
                # Normalize code to match format in unified_trade_data
                # Codes in unified_trade_data are padded to 10 digits, then sliced
                # So we need to pad codes to the appropriate length based on level
                code_str = str(code).strip()
                if level_int == 2:
                    normalized_code = code_str.zfill(2)  # "01" -> "01"
                elif level_int == 4:
                    normalized_code = code_str.zfill(4)  # "0101" -> "0101"
                elif level_int == 6:
                    normalized_code = code_str.zfill(6)  # "010121" -> "010121"
                elif level_int == 8:
                    normalized_code = code_str.zfill(8)  # "01012100" -> "01012100"
                elif level_int == 10:
                    normalized_code = code_str.zfill(10)  # "0101210000" -> "0101210000"
                else:
                    normalized_code = code_str
                
                tnved_refs.append({
                    'TNVED_CODE': normalized_code,
                    'TNVED_LEVEL': level_int,
                    'TNVED_NAME': name
                })
        
        if tnved_refs:
            tnved_df = pd.DataFrame(tnved_refs)
            conn.register('tnved_ref_df', tnved_df)
            conn.execute("""
                CREATE TABLE tnved_reference AS 
                SELECT DISTINCT TNVED_CODE, TNVED_LEVEL, TNVED_NAME
                FROM tnved_ref_df
                ORDER BY TNVED_LEVEL, TNVED_CODE
            """)
            conn.unregister('tnved_ref_df')
            logger.info(f"  ... created tnved_reference table with {len(tnved_df)} rows")
            
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
    
    # Create convenience view that joins main table with reference tables
    logger.info("Creating convenience view with joined reference data...")
    conn.execute("""
        CREATE OR REPLACE VIEW unified_trade_data_enriched AS
        SELECT 
            t.*,
            c.STRANA_NAME AS COUNTRY_NAME,
            t2.TNVED_NAME AS TNVED2_NAME,
            t4.TNVED_NAME AS TNVED4_NAME,
            t6.TNVED_NAME AS TNVED6_NAME,
            t8.TNVED_NAME AS TNVED8_NAME,
            COALESCE(t10.TNVED_NAME, t8.TNVED_NAME) AS TNVED_NAME,
            ROW_NUMBER() OVER (
                PARTITION BY t.STRANA
                ORDER BY t.PERIOD DESC
            ) AS period_rank
        FROM unified_trade_data t
        LEFT JOIN country_reference c ON t.STRANA = c.STRANA
        LEFT JOIN tnved_reference t2 ON t.TNVED2 = t2.TNVED_CODE AND t2.TNVED_LEVEL = 2
        LEFT JOIN tnved_reference t4 ON t.TNVED4 = t4.TNVED_CODE AND t4.TNVED_LEVEL = 4
        LEFT JOIN tnved_reference t6 ON t.TNVED6 = t6.TNVED_CODE AND t6.TNVED_LEVEL = 6
        LEFT JOIN tnved_reference t8 ON t.TNVED8 = t8.TNVED_CODE AND t8.TNVED_LEVEL = 8
        LEFT JOIN tnved_reference t10 ON t.TNVED = t10.TNVED_CODE AND t10.TNVED_LEVEL = 10
    """)
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
            'M²': canonical_records.get('КВАДРАТНЫЙ МЕТР'),
            'M3': canonical_records.get('КУБИЧЕСКИЙ МЕТР'),
            '2U': canonical_records.get('ПАРА'),
            'CARAT': canonical_records.get('МЕТРИЧЕСКИЙ КАРАТ(1КАРАТ=2*10(-4)КГ'),
            '1000U': canonical_records.get('ТЫСЯЧА ШТУК'),
            'G': canonical_records.get('ГРАММ'),
            '1000 KWH': canonical_records.get('1000 КИЛОВАТТ-ЧАС'),
            '1000 L': canonical_records.get('1000 ЛИТРОВ'),
            '1000 KG': canonical_records.get('ТОННА, МЕТРИЧЕСКАЯ ТОННА (1000 КГ)'),
            'L ALC 100%': canonical_records.get('ЛИТР ЧИСТОГО (100%) СПИРТА'),

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
        }
        
        final_mapping.update(aliases)
        
        # Filter out any None values that may have resulted from missing keys
        final_mapping = {k: v for k, v in final_mapping.items() if v is not None and k is not None}

        logger.info(f"Loaded common EDIZM mapping with {len(final_mapping)} case-insensitive keys.")
        return final_mapping
    except Exception as e:
        logger.error(f"Failed to load common EDIZM mapping: {e}")
        return {}

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

def load_tnved_mapping(project_root: Path) -> Dict[str, Dict[str, str]]:
    """Loads TNVED code to name mappings from tnved.csv."""
    mapping_file = project_root / "metadata" / "tnved.csv"
    if not mapping_file.exists():
        logger.error(f"TNVED mapping file not found at {mapping_file}")
        return {}

    try:
        df = pd.read_csv(mapping_file, dtype={'KOD': str, 'NAME': str, 'level': int})
        df.columns = df.columns.str.upper()

        mappings = {
            'tnved2': df[df['LEVEL'] == 2].set_index('KOD')['NAME'].to_dict(),
            'tnved4': df[df['LEVEL'] == 4].set_index('KOD')['NAME'].to_dict(),
            'tnved6': df[df['LEVEL'] == 6].set_index('KOD')['NAME'].to_dict(),
            'tnved8': df[df['LEVEL'] == 8].set_index('KOD')['NAME'].to_dict(),
            'tnved10': df[df['LEVEL'] == 10].set_index('KOD')['NAME'].to_dict()
        }
        
        logger.info("Successfully loaded TNVED mappings for all levels.")
        return mappings
    except Exception as e:
        logger.error(f"Failed to load TNVED mapping: {e}")
        return {}

def load_and_transform_comtrade(
    comtrade_db_path: Path,
    project_root: Path,
    exclude_countries: List[str],
    start_year: int = None
) -> pd.DataFrame:
    """
    Loads and transforms Comtrade data, excluding specified countries.

    Args:
        comtrade_db_path: Path to the Comtrade DuckDB database.
        project_root: Path to the project root for metadata loading.
        exclude_countries: List of ISO2 country codes to exclude.

    Returns:
        A DataFrame with Comtrade data transformed to the unified schema.
    """
    logger.info(f"Loading Comtrade data, excluding: {exclude_countries}")
    
    partner_mapping = load_partner_mapping(project_root)
    if not partner_mapping:
        return pd.DataFrame()
    
    edizm_mapping = load_edizm_mapping(project_root)
    if not edizm_mapping:
        logger.error("Could not load Edizm mapping, aborting Comtrade load.")
        return pd.DataFrame()

    # Convert ISO2 country codes to Comtrade M49 codes for the query
    # Create case-insensitive mapping: uppercase ISO2 -> M49
    country_to_m49 = {v.upper(): k for k, v in partner_mapping.items() if v}
    
    # Convert exclude list to uppercase and map to M49
    exclude_countries_upper = [c.upper() for c in exclude_countries]
    exclude_m49_codes = []
    for c in exclude_countries_upper:
        if c in country_to_m49:
            m49_code = country_to_m49[c]
            exclude_m49_codes.append(m49_code)
            logger.info(f"Excluding country '{c}' (M49 code: {m49_code}) from Comtrade data")
        else:
            logger.warning(f"Could not find M49 code for country: {c}")
    
    logger.info(f"Total countries to exclude from Comtrade: {len(exclude_m49_codes)}")

    try:
        conn = duckdb.connect(str(comtrade_db_path), read_only=True)
        
        # Diagnostic: List tables and schema in the database
        tables = conn.execute("SHOW TABLES;").fetchall()
        logger.info(f"Tables found in {comtrade_db_path}: {tables}")
        try:
            table_info = conn.execute("DESCRIBE comtrade_data;").df()
            logger.info(f"Schema for comtrade_data:\n{table_info}")
        except Exception as e:
            logger.warning(f"Could not describe comtrade_data table: {e}")
        
        # Build query with safe formatting (M49 codes are always integers, so safe to format)
        query_parts = ["SELECT"]
        query_parts.append("    period AS PERIOD,")
        query_parts.append("    reporterCode AS STRANA_CODE,")
        query_parts.append("    cmdCode AS TNVED,")
        query_parts.append("    CASE flowCode WHEN 'M' THEN 'ЭК' WHEN 'X' THEN 'ИМ' WHEN 'ЭК' THEN 'ЭК' WHEN 'ИМ' THEN 'ИМ' END AS NAPR,")
        query_parts.append("    qtyUnitCode,")
        query_parts.append("    altQtyUnitCode,")
        query_parts.append("    primaryValue AS STOIM,")
        query_parts.append("    netWgt AS NETTO,")
        query_parts.append("    qty,")
        query_parts.append("    altQty")
        query_parts.append("FROM comtrade_data")
        
        where_clauses = []
        
        if exclude_m49_codes:
            # M49 codes are always integers, safe to format directly
            # Validate all codes are integers for safety
            if not all(isinstance(code, int) for code in exclude_m49_codes):
                raise ValueError("All exclude_m49_codes must be integers")
            codes_str = ', '.join(map(str, exclude_m49_codes))
            where_clauses.append(f"reporterCode NOT IN ({codes_str})")
        
        if start_year:
            logger.info(f"Applying start_year filter to Comtrade data: year >= {start_year}")
            if not isinstance(start_year, int):
                raise ValueError("start_year must be an integer")
            where_clauses.append(f"refYear >= {start_year}")
        
        if where_clauses:
            query_parts.append("WHERE " + " AND ".join(where_clauses))
        
        query = "\n".join(query_parts)
        logger.info(f"Executing Comtrade query...")
        comtrade_df = conn.execute(query).fetchdf()
    except Exception as e:
        logger.error(f"Failed to query Comtrade data: {e}")
        return pd.DataFrame()
    finally:
        if 'conn' in locals():
            conn.close()
            
    if comtrade_df.empty:
        logger.info("Query returned no Comtrade data for the specified countries.")
        return pd.DataFrame()
    
    logger.info(f"Query returned {len(comtrade_df)} rows from Comtrade DB.")
        
    # Post-processing transformations
    logger.info("Transforming Comtrade data...")

    # Choose the non-weight quantity as the primary supplementary quantity (KOL).
    # The Comtrade code for Kilogram is 8.
    # If the primary quantity unit (`qtyUnitCode`) is KG, we prefer the alternate quantity.
    # Otherwise, we stick with the primary quantity.
    # This logic assumes altQty and altQtyUnitCode columns exist in comtrade_data.
    if 'altQtyUnitCode' in comtrade_df.columns and 'altQty' in comtrade_df.columns:
        use_alt_quantity = comtrade_df['qtyUnitCode'] == 8
        comtrade_df['EDIZM_CODE'] = comtrade_df['altQtyUnitCode'].where(use_alt_quantity, comtrade_df['qtyUnitCode'])
        comtrade_df['KOL'] = comtrade_df['altQty'].where(use_alt_quantity, comtrade_df['qty'])
    else:
        logger.warning("altQtyUnitCode or altQty not found in Comtrade data. Using primary quantity fields.")
        comtrade_df['EDIZM_CODE'] = comtrade_df['qtyUnitCode']
        comtrade_df['KOL'] = comtrade_df['qty']

    comtrade_df['STRANA'] = comtrade_df['STRANA_CODE'].map(partner_mapping)
    
    # Ensure STRANA is uppercase for consistency
    comtrade_df['STRANA'] = comtrade_df['STRANA'].str.upper()
    
    comtrade_df['EDIZM'] = comtrade_df['EDIZM_CODE'].map(edizm_mapping)
    comtrade_df.fillna({'EDIZM': 'N/A'}, inplace=True)
    
    null_strana_count = comtrade_df['STRANA'].isnull().sum()
    if null_strana_count > 0:
        logger.warning(f"Found {null_strana_count} rows with reporter codes that could not be mapped to ISO2 codes. These will be dropped.")
        unmapped_codes = comtrade_df[comtrade_df['STRANA'].isnull()]['STRANA_CODE'].unique()
        logger.warning(f"Unmapped reporter codes (sample): {unmapped_codes[:10]}")

    comtrade_df.dropna(subset=['STRANA'], inplace=True)
    logger.info(f"{len(comtrade_df)} rows remaining after dropping unmapped countries.")
    
    # Verify unique countries in Comtrade data
    comtrade_countries = comtrade_df['STRANA'].unique()
    logger.info(f"Countries in Comtrade data after transformation: {sorted(comtrade_countries)}")

    if comtrade_df.empty:
        logger.warning("No Comtrade data remaining after transformation.")
        return pd.DataFrame()
        
    comtrade_df['EDIZM_ISO'] = None

    # Generate derived TNVED columns
    # Remove leading zeros first, then pad to 10 digits on the RIGHT
    comtrade_df['TNVED'] = comtrade_df['TNVED'].astype(str).str.strip()
    comtrade_df['TNVED'] = comtrade_df['TNVED'].str.lstrip('0')
    comtrade_df.loc[comtrade_df['TNVED'] == '', 'TNVED'] = '0'
    def pad_right(code):
        if len(code) >= 10:
            return code[:10]
        return code + '0' * (10 - len(code))
    comtrade_df['TNVED'] = comtrade_df['TNVED'].apply(pad_right)
    comtrade_df['TNVED2'] = comtrade_df['TNVED'].str.slice(0, 2)
    comtrade_df['TNVED4'] = comtrade_df['TNVED'].str.slice(0, 4)
    comtrade_df['TNVED6'] = comtrade_df['TNVED'].str.slice(0, 6)
    comtrade_df['TNVED8'] = comtrade_df['TNVED'].str.slice(0, 8)
    
    # Ensure data types match the expected schema
    for col, expected_type in EXPECTED_SCHEMA.items():
        if col in comtrade_df.columns and str(comtrade_df[col].dtype) != expected_type:
            try:
                if 'datetime' in expected_type:
                    comtrade_df[col] = pd.to_datetime(comtrade_df[col])
                else:
                    comtrade_df[col] = comtrade_df[col].astype(expected_type)
            except (ValueError, TypeError) as e:
                logger.warning(f"Could not convert Comtrade column '{col}' to {expected_type}: {e}")

    # Reorder columns to match the main schema
    final_cols = [col for col in EXPECTED_SCHEMA.keys() if col in comtrade_df.columns]
    return comtrade_df[final_cols]
    
def main():
    """Main function to orchestrate the merging process."""
    parser = argparse.ArgumentParser(
        description="Merge processed national data and optionally include Comtrade data."
    )
    parser.add_argument(
        '--include-comtrade',
        action='store_true',
        help="Include Comtrade data for countries not present in national data."
    )
    parser.add_argument(
        '--start-year',
        type=int,
        default=None,
        help="Filter all data to include records from this year onwards."
    )
    parser.add_argument(
        '--exclude-countries',
        type=str,
        nargs='+',
        default=[],
        help="List of ISO2 country codes to exclude from the merge (e.g., IN CN)."
    )
    parser.add_argument(
        '--keep-outliers',
        action='store_true',
        help="Keep outliers as-is instead of replacing them with NaN (default: replace outliers with NaN)."
    )
    parser.add_argument(
        '--skip-outlier-detection',
        action='store_true',
        help="Skip outlier detection and replacement entirely (default: detect and replace outliers)."
    )
    args = parser.parse_args()

    # Define paths using the script's location for robustness
    project_root = Path(__file__).resolve().parent.parent
    data_processed_dir = project_root / "data_processed"
    db_dir = project_root / "db"
    output_db_path = db_dir / "unified_trade_data.duckdb"
    comtrade_db_path = db_dir / "comtrade.db"
    
    # Ensure output directory exists
    db_dir.mkdir(exist_ok=True)
    
    logger.info("Starting data merging process...")
    
    # Find all parquet files in data_processed
    parquet_files = list(data_processed_dir.glob("*.parquet"))
    logger.info(f"Found {len(parquet_files)} parquet files: {[f.name for f in parquet_files]}")
    
    excluded_countries_upper = [c.upper() for c in args.exclude_countries]

    # Load and validate national datasets
    national_datasets = {}
    if parquet_files:
        for file_path in parquet_files:
            country_code = file_path.stem.replace('_full', '').upper()
            if country_code in excluded_countries_upper:
                logger.info(f"Skipping {file_path.name} as per --exclude-countries argument.")
                continue

            df = load_and_validate_file(file_path, start_year=args.start_year)
            if df is not None:
                df_processed = generate_derived_columns(df)
                # Ensure STRANA is uppercase for consistency
                if 'STRANA' in df_processed.columns:
                    df_processed['STRANA'] = df_processed['STRANA'].str.upper()
                national_datasets[country_code.lower()] = df_processed
    else:
        logger.warning("No national parquet files found in data_processed directory.")

    all_dataframes = []
    national_countries_iso = []

    # Process national data
    for source_name, df in national_datasets.items():
        df['SOURCE'] = 'national'
        all_dataframes.append(df)
        if 'STRANA' in df.columns and not df.empty:
            # Get all unique country codes from this dataset and ensure uppercase
            unique_countries = df['STRANA'].dropna().unique()
            for country in unique_countries:
                country_upper = country.upper()
                if country_upper not in national_countries_iso:
                    national_countries_iso.append(country_upper)
    
    # Process Comtrade data if flag is set
    if args.include_comtrade:
        if not comtrade_db_path.exists():
            logger.error(f"Comtrade database not found at {comtrade_db_path}. Cannot include Comtrade data.")
        else:
            # Always exclude national data countries from Comtrade pull
            # And also add any user-specified exclusions
            countries_to_exclude_from_comtrade = list(set(national_countries_iso + excluded_countries_upper))
            logger.info(f"Excluding countries from Comtrade data to avoid duplicates: {countries_to_exclude_from_comtrade}")

            comtrade_df = load_and_transform_comtrade(
                comtrade_db_path, 
                project_root, 
                exclude_countries=countries_to_exclude_from_comtrade,
                start_year=args.start_year
            )
            if not comtrade_df.empty:
                # Double-check: filter out any national countries that might have slipped through
                initial_comtrade_rows = len(comtrade_df)
                indices_to_drop = comtrade_df[comtrade_df['STRANA'].isin(national_countries_iso)].index
                comtrade_df.drop(indices_to_drop, inplace=True)
                filtered_rows = initial_comtrade_rows - len(comtrade_df)
                if filtered_rows > 0:
                    logger.info(f"Filtered {filtered_rows:,} duplicate rows from Comtrade data that matched national countries.")
                
                comtrade_df['SOURCE'] = 'comtrade'
                all_dataframes.append(comtrade_df)

    if not all_dataframes:
        logger.error("No data available to merge.")
        return

    # Merge all datasets
    merged_df = pd.concat(all_dataframes, ignore_index=True)
    
    # Apply country exclusions to the final merged dataset
    if excluded_countries_upper:
        initial_rows = len(merged_df)
        indices_to_drop = merged_df[merged_df['STRANA'].isin(excluded_countries_upper)].index
        merged_df.drop(indices_to_drop, inplace=True)
        excluded_rows = initial_rows - len(merged_df)
        if excluded_rows > 0:
            logger.info(f"Excluded {excluded_rows:,} rows for countries: {excluded_countries_upper}")
    
    merged_df = merged_df.sort_values(['PERIOD', 'STRANA', 'TNVED'])
    
    # Remove rows where NAPR is NULL
    initial_rows = len(merged_df)
    merged_df.dropna(subset=['NAPR'], inplace=True)
    null_napr_rows = initial_rows - len(merged_df)
    if null_napr_rows > 0:
        logger.info(f"Removed {null_napr_rows:,} rows with NULL NAPR values")

    # Standardize EDIZM column
    logger.info("Standardizing EDIZM column...")
    common_edizm_map = load_common_edizm_mapping(project_root)
    if common_edizm_map:
        # Normalize original EDIZM values before mapping (astype(str) is crucial)
        merged_df['EDIZM_upper'] = merged_df['EDIZM'].astype(str).str.upper().str.strip()
        
        # Map to common representation
        mapped_values = merged_df['EDIZM_upper'].map(common_edizm_map)
        
        # Update EDIZM and EDIZM_ISO
        merged_df['EDIZM'] = mapped_values.map(lambda x: x['NAME'] if pd.notna(x) else None)
        merged_df['EDIZM_ISO'] = mapped_values.map(lambda x: x['KOD'] if pd.notna(x) else None)
        
        # Handle unmapped values
        unmapped_mask = merged_df['EDIZM'].isnull()
        if unmapped_mask.sum() > 0:
            logger.warning(f"{unmapped_mask.sum()} EDIZM values could not be mapped to a common standard.")
            unmapped_sample = merged_df[unmapped_mask]['EDIZM_upper'].unique()
            logger.warning(f"Unmapped EDIZM sample: {unmapped_sample[:10]}")
            
        merged_df.drop(columns=['EDIZM_upper'], inplace=True)
    else:
        logger.error("Could not standardize EDIZM values due to mapping load failure.")

    # Nullify KOL where EDIZM is KG to avoid duplication with NETTO
    logger.info("Checking for supplementary units in KG to avoid duplication with NETTO...")
    kg_iso_code = '166'  # ISO code for Kilogram
    if 'EDIZM_ISO' in merged_df.columns:
        # Use .loc to avoid SettingWithCopyWarning
        kg_rows_mask = merged_df['EDIZM_ISO'] == kg_iso_code
        num_kg_rows = kg_rows_mask.sum()

        if num_kg_rows > 0:
            logger.info(f"Found {num_kg_rows:,} rows where the supplementary unit is KG. "
                        f"Setting KOL, EDIZM, and EDIZM_ISO to NULL for these rows.")
            merged_df.loc[kg_rows_mask, 'KOL'] = None
            merged_df.loc[kg_rows_mask, 'EDIZM'] = None
            merged_df.loc[kg_rows_mask, 'EDIZM_ISO'] = None
    else:
        logger.warning("Cannot perform KG duplication check: EDIZM_ISO column not found.")

    # Handle Tonnes: convert to KG if NETTO is missing, otherwise nullify to avoid duplication
    logger.info("Checking for supplementary units in Tonnes to convert or remove...")
    tonne_iso_code = '168'
    if 'EDIZM_ISO' in merged_df.columns:
        tonne_mask = (merged_df['EDIZM_ISO'] == tonne_iso_code) & merged_df['KOL'].notna()
        num_tonne_rows = tonne_mask.sum()

        if num_tonne_rows > 0:
            logger.info(f"Found {num_tonne_rows:,} rows with supplementary unit in Tonnes.")
            
            # Case 1: NETTO is missing or zero, so we can backfill it from KOL
            netto_missing_mask = tonne_mask & ((merged_df['NETTO'].isnull()) | (merged_df['NETTO'] == 0))
            num_to_convert = netto_missing_mask.sum()
            if num_to_convert > 0:
                logger.info(f"  - Converting {num_to_convert:,} Tonne values to KG and filling NETTO.")
                # Convert Tonnes in KOL to KG and assign to NETTO
                merged_df.loc[netto_missing_mask, 'NETTO'] = merged_df.loc[netto_missing_mask, 'KOL'] * 1000
                # Nullify the supplementary unit columns as the value is now in NETTO
                merged_df.loc[netto_missing_mask, 'KOL'] = None
                merged_df.loc[netto_missing_mask, 'EDIZM'] = None
                merged_df.loc[netto_missing_mask, 'EDIZM_ISO'] = None

            # Case 2: NETTO already has a value, so KOL is redundant
            # Re-calculate the mask to only affect rows not already handled above
            tonne_mask = (merged_df['EDIZM_ISO'] == tonne_iso_code) & merged_df['KOL'].notna()
            netto_present_mask = tonne_mask & merged_df['NETTO'].notna() & (merged_df['NETTO'] != 0)
            num_to_remove = netto_present_mask.sum()
            if num_to_remove > 0:
                logger.info(f"  - Removing {num_to_remove:,} redundant Tonne values as NETTO is already populated.")
                merged_df.loc[netto_present_mask, 'KOL'] = None
                merged_df.loc[netto_present_mask, 'EDIZM'] = None
                merged_df.loc[netto_present_mask, 'EDIZM_ISO'] = None
    else:
        logger.warning("Cannot perform Tonne duplication check: EDIZM_ISO column not found.")

    # Note: Country names and TNVED names are now stored in separate reference tables
    # and can be joined via the unified_trade_data_enriched view or directly in queries
    logger.info("Reference tables (country names, TNVED names) will be created as separate tables in the database.")

    # Detect outliers in merged data by time series (as in outlier_detection.Rmd)
    # Пропускаем, если указан флаг --skip-outlier-detection
    if args.skip_outlier_detection:
        logger.info("Skipping outlier detection and replacement (--skip-outlier-detection flag set)")
    else:
        # Используем параметры из outlier_detection.Rmd: nsd=6, tv=10^6
        logger.info("Detecting outliers in KOL column by time series (STRANA, TNVED, NAPR)...")
        logger.info("Using parameters from outlier_detection.Rmd: nsd=6.0, tv=1e6")
        
        # Сохраняем копию данных до замены выбросов для отчета
        merged_df_before_outlier_replacement = merged_df.copy()
        
        outlier_ts_results = detect_outliers_by_time_series(
            merged_df,
            nsd=6.0,  # 6 стандартных отклонений (как в outlier_detection.Rmd)
            tv=1e6,   # Порог 10^6 (как в outlier_detection.Rmd)
            require_all_methods=True  # Только ряды, где все 3 метода нашли выбросы
        )
        
        logger.info("=== OUTLIER DETECTION BY TIME SERIES ===")
        if not outlier_ts_results.empty:
            logger.info(f"Found {len(outlier_ts_results)} time series with outliers (all 3 methods)")
            logger.info(f"Total outliers detected:")
            logger.info(f"  - Method 1 (KOL): {outlier_ts_results['outliers_1'].sum():,}")
            logger.info(f"  - Method 2 (KOL/STOIM): {outlier_ts_results['outliers_2'].sum():,}")
            logger.info(f"  - Method 3 (KOL/NETTO): {outlier_ts_results['outliers_3'].sum():,}")
            
            # Показываем топ-10 рядов с наибольшим количеством выбросов
            outlier_ts_results['total_outliers'] = (
                outlier_ts_results['outliers_1'] + 
                outlier_ts_results['outliers_2'] + 
                outlier_ts_results['outliers_3']
            )
            top_outliers = outlier_ts_results.nlargest(10, 'total_outliers')
            logger.info("Top 10 time series with most outliers:")
            for _, row in top_outliers.iterrows():
                logger.info(
                    f"  {row['STRANA']} / {row['TNVED']} / {row['NAPR']}: "
                    f"{row['outliers_1']} + {row['outliers_2']} + {row['outliers_3']} = {row['total_outliers']} total"
                )
        else:
            logger.info("No time series found with outliers detected by all 3 methods")
        
        # Заменяем выбросы на NaN (если не указан флаг --keep-outliers)
        replaced_count = 0
        if not args.keep_outliers and not outlier_ts_results.empty:
            logger.info("=== REPLACING OUTLIERS WITH NaN ===")
            initial_kol_count = merged_df['KOL'].notna().sum()
            merged_df = replace_outliers_with_nan(
                merged_df,
                outlier_ts_results,
                nsd=6.0,
                tv=1e6
            )
            final_kol_count = merged_df['KOL'].notna().sum()
            replaced_count = initial_kol_count - final_kol_count
            logger.info(f"Replaced {replaced_count:,} outlier values in KOL with NaN")
        elif args.keep_outliers:
            logger.info("Keeping outliers as-is (--keep-outliers flag set)")
        
        # Создаем и сохраняем отчет о выбросах
        if not outlier_ts_results.empty:
            logger.info("=== CREATING OUTLIER REPORT ===")
            # Создаем детальный отчет на основе данных до замены
            outlier_report = create_outlier_report(
                merged_df_before_outlier_replacement,
                outlier_ts_results,
                replaced_count=replaced_count,
                keep_outliers=args.keep_outliers,
                nsd=6.0,
                tv=1e6
            )
            
            # Сохраняем отчет в папку reports/
            reports_dir = project_root / 'reports'
            save_outlier_report(
                outlier_report,
                outlier_ts_results,
                replaced_count=replaced_count,
                keep_outliers=args.keep_outliers,
                output_dir=reports_dir,
                nsd=6.0,
                tv=1e6
            )
        
        # Также делаем общую проверку без группировки для статистики
        logger.info("=== OVERALL OUTLIER STATISTICS ===")
        overall_outliers = detect_outliers_in_dataframe(
            merged_df,
            nsd=6.0,
            tv_kol=1e6
        )
        for key, count in overall_outliers.items():
            logger.info(f"  {key}: {count:,} outliers")

    # Display summary statistics
    logger.info("=== MERGE SUMMARY ===")
    logger.info(f"Total rows: {len(merged_df)}")
    logger.info(f"Unique countries: {merged_df['STRANA'].nunique()}")
    logger.info(f"Date range: {merged_df['PERIOD'].min()} to {merged_df['PERIOD'].max()}")
    
    logger.info("Rows by source:")
    source_counts = merged_df['SOURCE'].value_counts()
    for source, count in source_counts.items():
        logger.info(f"  {source}: {count:,} rows")
        
    logger.info("Rows by country:")
    country_counts = merged_df.groupby('SOURCE')['STRANA'].value_counts()
    logger.info(str(country_counts))
    
    # Show EDIZM counts by country
    logger.info("EDIZM counts by country:")
    edizm_counts = merged_df.groupby(['STRANA', 'EDIZM']).size().reset_index(name='count')
    edizm_counts = edizm_counts.sort_values(['STRANA', 'count'], ascending=[True, False])
    for strana, group in edizm_counts.groupby('STRANA'):
        logger.info(f"  Country: {strana}")
        for _, row in group.head(5).iterrows(): # Log top 5 EDIZM for each country
            logger.info(f"    - {row['EDIZM']}: {row['count']:,} rows")

    # Save to DuckDB
    save_to_duckdb(merged_df, output_db_path)
    
    # Save reference tables and create convenience view
    conn = duckdb.connect(str(output_db_path))
    try:
        save_reference_tables(conn, project_root)
        conn.close()
    except Exception as e:
        conn.close()
        logger.error(f"Failed to create reference tables: {e}")
        raise

if __name__ == "__main__":
    main()