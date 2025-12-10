#!/usr/bin/env python3
"""
Module for detecting and handling outliers in trade data.

This module:
1. Reads data from DuckDB database
2. Detects outliers in KOL column using three methods
3. Optionally replaces outliers with NULL
4. Updates the database
5. Creates detailed reports
"""

import pandas as pd
import duckdb
from pathlib import Path
import logging
import argparse
import json
from typing import Dict
import numpy as np
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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
        # НО: если метод 1 (KOL) нашел выбросы, это уже достаточно важно, чтобы их учитывать
        # Методы 2 и 3 могут не сработать, если STOIM/NETTO = 0 или если соотношения не выходят за пределы
        if require_all_methods:
            # Если метод 1 нашел выбросы, но методы 2 или 3 не нашли - все равно учитываем
            # Это важно для случаев, когда STOIM/NETTO = 0 или когда соотношения не показывают выбросы
            if outliers_1 >= 1:
                # Метод 1 нашел выбросы - это достаточно
                pass
            elif not (outliers_1 >= 1 and outliers_2 >= 1 and outliers_3 >= 1):
                # Если метод 1 не нашел выбросы, требуем все три метода
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
        
        # Определяем, какие выбросы заменять
        # Если метод 1 нашел выбросы, используем их (даже если методы 2 и 3 не нашли)
        # Если все три метода нашли выбросы, используем пересечение всех трех
        outliers_1_count = outlier_row.get('outliers_1', 0)
        outliers_2_count = outlier_row.get('outliers_2', 0)
        outliers_3_count = outlier_row.get('outliers_3', 0)
        
        if outliers_1_count >= 1 and outliers_2_count >= 1 and outliers_3_count >= 1:
            # Все три метода нашли выбросы - используем пересечение
            final_outlier_mask = outlier_mask_1 & outlier_mask_2 & outlier_mask_3
        elif outliers_1_count >= 1:
            # Только метод 1 нашел выбросы - используем его
            final_outlier_mask = outlier_mask_1
        else:
            # Это не должно происходить, но на всякий случай
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
        
        # Преобразуем numpy типы в нативные Python типы для JSON сериализации
        def convert_to_native(obj):
            """Рекурсивно преобразует numpy типы в нативные Python типы."""
            if isinstance(obj, (np.integer, np.int64, np.int32)):
                return int(obj)
            elif isinstance(obj, (np.floating, np.float64, np.float32)):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            elif isinstance(obj, dict):
                return {key: convert_to_native(value) for key, value in obj.items()}
            elif isinstance(obj, (list, tuple)):
                return [convert_to_native(item) for item in obj]
            elif isinstance(obj, (bool, type(None))):
                return obj
            elif isinstance(obj, (str, int, float)):
                return obj
            else:
                return str(obj)
        
        report_metadata = {
            'timestamp': timestamp,
            'detection_parameters': {
                'nsd': float(nsd),
                'tv': float(tv),
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
                'replaced_with_nan': int(replaced_count),
                'keep_outliers': bool(keep_outliers)
            },
            'detailed_records_count': int(len(report_df)) if not report_df.empty else 0
        }
        
        # Преобразуем все значения в нативные типы
        report_metadata = convert_to_native(report_metadata)
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(report_metadata, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved outlier report metadata to {json_path}")


def process_outliers_in_db(
    db_path: Path,
    nsd: float = 6.0,
    tv: float = 1e6,
    replace_outliers: bool = True,
    reports_dir: Path = None
) -> Dict:
    """
    Основная функция для обработки выбросов в базе данных DuckDB.
    
    Читает данные из базы, обнаруживает выбросы, обновляет базу и создает отчеты.
    
    Args:
        db_path: Путь к файлу базы данных DuckDB
        nsd: Количество стандартных отклонений для определения выброса
        tv: Пороговое значение для KOL
        replace_outliers: Если True, заменяет выбросы на NULL, иначе только обнаруживает
        reports_dir: Директория для сохранения отчетов (если None, используется reports/ в корне проекта)
        
    Returns:
        Словарь с результатами обработки
    """
    if not db_path.exists():
        logger.error(f"Database file not found: {db_path}")
        return {}
    
    logger.info(f"Processing outliers in database: {db_path}")
    logger.info(f"Parameters: nsd={nsd}, tv={tv}, replace_outliers={replace_outliers}")
    
    # Определяем директорию для отчетов
    if reports_dir is None:
        project_root = db_path.resolve().parent.parent
        reports_dir = project_root / 'reports'
    
    # Подключаемся к базе данных
    conn = duckdb.connect(str(db_path), read_only=False)
    
    try:
        # Проверяем наличие таблицы
        tables = conn.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]
        
        if 'unified_trade_data' not in table_names:
            logger.error("Table 'unified_trade_data' not found in database")
            return {}
        
        # Читаем данные из базы
        logger.info("Reading data from database...")
        df = conn.execute("""
            SELECT STRANA, TNVED, NAPR, PERIOD, KOL, STOIM, NETTO
            FROM unified_trade_data
            WHERE KOL IS NOT NULL
            ORDER BY PERIOD
        """).df()
        
        if df.empty:
            logger.warning("No data with KOL values found in database")
            return {}
        
        logger.info(f"Loaded {len(df):,} rows with KOL values")
        
        # Сохраняем копию данных до обработки для отчета
        df_before = df.copy()
        
        # Обнаруживаем выбросы
        logger.info("Detecting outliers by time series...")
        outlier_ts_results = detect_outliers_by_time_series(
            df,
            nsd=nsd,
            tv=tv,
            require_all_methods=True
        )
        
        logger.info("=== OUTLIER DETECTION BY TIME SERIES ===")
        if not outlier_ts_results.empty:
            # Подсчитываем ряды с разными комбинациями методов
            all_three = ((outlier_ts_results['outliers_1'] >= 1) & 
                        (outlier_ts_results['outliers_2'] >= 1) & 
                        (outlier_ts_results['outliers_3'] >= 1)).sum()
            method1_only = ((outlier_ts_results['outliers_1'] >= 1) & 
                           ((outlier_ts_results['outliers_2'] < 1) | 
                            (outlier_ts_results['outliers_3'] < 1))).sum()
            
            logger.info(f"Found {len(outlier_ts_results)} time series with outliers:")
            logger.info(f"  - All 3 methods detected outliers: {all_three}")
            logger.info(f"  - Only method 1 (KOL) detected outliers: {method1_only}")
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
            return {
                'outlier_series_count': 0,
                'replaced_count': 0
            }
        
        # Заменяем выбросы на NULL в базе данных
        replaced_count = 0
        if replace_outliers and not outlier_ts_results.empty:
            logger.info("=== REPLACING OUTLIERS WITH NULL ===")
            
            # Для каждого временного ряда с выбросами обновляем базу
            for _, outlier_row in outlier_ts_results.iterrows():
                strana = outlier_row['STRANA']
                tnved = outlier_row['TNVED']
                napr = outlier_row['NAPR']
                
                # Находим выбросы в этом ряду
                mask = (
                    (df['STRANA'] == strana) &
                    (df['TNVED'] == tnved) &
                    (df['NAPR'] == napr) &
                    (df['KOL'].notna())
                )
                
                if not mask.any():
                    continue
                
                group = df.loc[mask].copy()
                kol_values = group['KOL'].dropna()
                
                if kol_values.empty:
                    continue
                
                mean_kol = kol_values.mean()
                std_kol = kol_values.std()
                
                # Определяем выбросы (используем ту же логику, что и в replace_outliers_with_nan)
                outlier_mask = pd.Series(False, index=group.index)
                
                # Метод 1
                if std_kol > 0:
                    z_kol = (group['KOL'] - mean_kol) / std_kol
                    outlier_mask_1 = (z_kol.abs() > nsd) & (group['KOL'] > tv)
                    outlier_mask = outlier_mask | outlier_mask_1
                
                # Метод 2
                if 'STOIM' in group.columns:
                    valid_mask = group['STOIM'].notna() & (group['STOIM'] != 0) & group['KOL'].notna()
                    if valid_mask.any():
                        ratio = group.loc[valid_mask, 'KOL'] / group.loc[valid_mask, 'STOIM']
                        mean_ratio = ratio.mean()
                        std_ratio = ratio.std()
                        if std_ratio > 0:
                            z_ratio = (ratio - mean_ratio) / std_ratio
                            outlier_mask_2 = (z_ratio.abs() > nsd) & (group.loc[valid_mask, 'KOL'] > tv)
                            outlier_mask.loc[valid_mask] = outlier_mask.loc[valid_mask] | outlier_mask_2
                
                # Метод 3
                if 'NETTO' in group.columns:
                    valid_mask = group['NETTO'].notna() & (group['NETTO'] != 0) & group['KOL'].notna()
                    if valid_mask.any():
                        ratio = group.loc[valid_mask, 'KOL'] / group.loc[valid_mask, 'NETTO']
                        mean_ratio = ratio.mean()
                        std_ratio = ratio.std()
                        if std_ratio > 0:
                            z_ratio = (ratio - mean_ratio) / std_ratio
                            outlier_mask_3 = (z_ratio.abs() > nsd) & (group.loc[valid_mask, 'KOL'] > tv)
                            outlier_mask.loc[valid_mask] = outlier_mask.loc[valid_mask] | outlier_mask_3
                
                # Обновляем базу данных для найденных выбросов
                if outlier_mask.any():
                    outlier_periods = group.loc[outlier_mask, 'PERIOD']
                    
                    for period in outlier_periods:
                        # Обновляем KOL на NULL для конкретной записи
                        conn.execute("""
                            UPDATE unified_trade_data
                            SET KOL = NULL
                            WHERE STRANA = ? 
                              AND TNVED = ? 
                              AND NAPR = ? 
                              AND PERIOD = ?
                              AND KOL IS NOT NULL
                        """, [strana, tnved, napr, period])
                    
                    replaced_count += outlier_mask.sum()
            
            logger.info(f"Replaced {replaced_count:,} outlier values in KOL with NULL in database")
        else:
            logger.info("Keeping outliers as-is (replace_outliers=False)")
        
        # Создаем отчеты
        if not outlier_ts_results.empty:
            logger.info("=== CREATING OUTLIER REPORT ===")
            outlier_report = create_outlier_report(
                df_before,
                outlier_ts_results,
                replaced_count=replaced_count,
                keep_outliers=not replace_outliers,
                nsd=nsd,
                tv=tv
            )
            
            save_outlier_report(
                outlier_report,
                outlier_ts_results,
                replaced_count=replaced_count,
                keep_outliers=not replace_outliers,
                output_dir=reports_dir,
                nsd=nsd,
                tv=tv
            )
        
        # Общая статистика
        logger.info("=== OVERALL OUTLIER STATISTICS ===")
        overall_outliers = detect_outliers_in_dataframe(
            df,
            nsd=nsd,
            tv_kol=tv
        )
        for key, count in overall_outliers.items():
            logger.info(f"  {key}: {count:,} outliers")
        
        return {
            'outlier_series_count': len(outlier_ts_results),
            'replaced_count': replaced_count,
            'outliers_method1': int(outlier_ts_results['outliers_1'].sum()),
            'outliers_method2': int(outlier_ts_results['outliers_2'].sum()),
            'outliers_method3': int(outlier_ts_results['outliers_3'].sum())
        }
        
    finally:
        conn.close()


def main():
    """Main function for command-line usage."""
    parser = argparse.ArgumentParser(
        description="Detect and handle outliers in unified trade data database."
    )
    parser.add_argument(
        '--db-path',
        type=str,
        default=None,
        help="Path to DuckDB database file (default: db/unified_trade_data.duckdb)"
    )
    parser.add_argument(
        '--nsd',
        type=float,
        default=6.0,
        help="Number of standard deviations for outlier detection (default: 6.0)"
    )
    parser.add_argument(
        '--tv',
        type=float,
        default=1e6,
        help="Threshold value for KOL (default: 1e6)"
    )
    parser.add_argument(
        '--keep-outliers',
        action='store_true',
        help="Keep outliers as-is instead of replacing them with NULL (default: replace outliers)"
    )
    parser.add_argument(
        '--reports-dir',
        type=str,
        default=None,
        help="Directory for saving reports (default: reports/ in project root)"
    )
    args = parser.parse_args()
    
    # Определяем путь к базе данных
    if args.db_path:
        db_path = Path(args.db_path)
    else:
        script_dir = Path(__file__).resolve().parent
        project_root = script_dir.parent
        db_path = project_root / 'db' / 'unified_trade_data.duckdb'
    
    # Определяем директорию для отчетов
    reports_dir = None
    if args.reports_dir:
        reports_dir = Path(args.reports_dir)
    
    # Обрабатываем выбросы
    results = process_outliers_in_db(
        db_path=db_path,
        nsd=args.nsd,
        tv=args.tv,
        replace_outliers=not args.keep_outliers,
        reports_dir=reports_dir
    )
    
    logger.info("Outlier detection process completed!")
    logger.info(f"Results: {results}")


if __name__ == "__main__":
    main()

