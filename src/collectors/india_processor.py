# -*- coding: utf-8 -*-
"""Процессор данных Индии: объединяет сырые CSV MEIDB в in_full.parquet."""

import pandas as pd
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from collectors._base import get_project_root, setup_logging
from core.country_processor_contract import (
    CountryProcessorInput,
    assert_country_output_contract,
    finalize_country_output,
    save_country_output,
)
from core.edizm import load_common_edizm_mapping, resolve_edizm_records

logger = setup_logging(__name__)

# MEIDB меняет единицы USD в выгрузке (тыс. vs млн). Порог по сумме файла надёжнее даты.
_STOIM_MILLIONS_USD_SUM_THRESHOLD = 100_000


def infer_india_stoim_multiplier(stoim: pd.Series) -> float:
    """Return 1000 when raw STOIM is in million USD, else 1 (already thousand USD)."""
    values = pd.to_numeric(stoim, errors="coerce").fillna(0)
    total = values.sum()
    if total <= 0:
        return 1.0
    # Monthly MEIDB files in thousand USD have totals in the millions; million-USD files ~3k–7k.
    if values.max() >= _STOIM_MILLIONS_USD_SUM_THRESHOLD:
        return 1.0
    return 1000.0 if total < _STOIM_MILLIONS_USD_SUM_THRESHOLD else 1.0


# Plausible band for the median unit value (STOIM in USD / NETTO in kg) after scaling.
# Observed India monthly medians are ~1.2–7.5 USD/kg across 2018–2026 (incl. the 2026
# switch to million-USD source, correctly scaled); a 1000x million/thousand misscale
# would land at ~0.007 or ~7000, far outside this band.
_INDIA_UNIT_VALUE_MIN_USD_PER_KG = 0.1
_INDIA_UNIT_VALUE_MAX_USD_PER_KG = 1000.0


def assert_india_stoim_unit_value_sane(
    stoim_usd: pd.Series, netto_kg: pd.Series, *, label: str = ""
) -> float:
    """Fail loudly if the median unit value is implausible after STOIM scaling.

    infer_india_stoim_multiplier picks the million/thousand-USD multiplier from file-sum
    magnitude, which could mis-scale an unusually small or large month. Instead of letting
    a 1000x misscale enter the data silently, validate the OUTCOME: after STOIM is scaled
    to USD, the median STOIM/NETTO over positive-NETTO rows must sit inside
    [_INDIA_UNIT_VALUE_MIN_USD_PER_KG, _INDIA_UNIT_VALUE_MAX_USD_PER_KG]. Catches a misscale
    in either direction. Returns the median (NaN when there is nothing to check).
    """
    stoim = pd.to_numeric(stoim_usd, errors="coerce")
    netto = pd.to_numeric(netto_kg, errors="coerce")
    mask = (netto > 0) & stoim.notna() & (stoim > 0)
    if not mask.any():
        return float("nan")
    unit_value = float((stoim[mask] / netto[mask]).median())
    if not (_INDIA_UNIT_VALUE_MIN_USD_PER_KG <= unit_value <= _INDIA_UNIT_VALUE_MAX_USD_PER_KG):
        raise ValueError(
            f"India MEIDB STOIM scale looks wrong for {label or 'this file'}: "
            f"median unit value {unit_value:,.4f} USD/kg is outside the plausible "
            f"[{_INDIA_UNIT_VALUE_MIN_USD_PER_KG}, {_INDIA_UNIT_VALUE_MAX_USD_PER_KG}] band "
            "(likely a 1000x million/thousand-USD misscale). Check the source units."
        )
    return unit_value


def process_and_merge_india_data(raw_data_dir: Path, output_file: Path, edizm_file: Path):
    """
    Сканирует директорию с необработанными данными, обрабатывает каждый CSV-файл,
    объединяет их в один DataFrame и сохраняет в формате Parquet.
    """

    logger.info("=== Начало обработки данных Индии ===")

    processor_input = CountryProcessorInput.from_paths(
        raw_data_dir,
        output_file,
        country_code="IN",
        edizm_file=edizm_file,
    )
    
    project_root = processor_input.metadata_dir.parent if processor_input.metadata_dir else Path.cwd()
    common_edizm_map = load_common_edizm_mapping(project_root)

    all_files = sorted(processor_input.raw_data_dir.glob("india_*.csv"))
    if not all_files:
        logger.error(f"Не найдено файлов в {processor_input.raw_data_dir}")
        return

    logger.info(f"Найдено {len(all_files)} файлов для объединения")

    dfs = []
    for file_path in all_files:
        try:
            df = pd.read_csv(file_path, dtype={
                'TNVED': str,
                'TNVED2': str,
                'TNVED4': str,
                'TNVED6': str
            })
            logger.info(f"  → {file_path.name}: {len(df)} строк")

            df['PERIOD'] = pd.to_datetime(
                df['Year'].astype(str) + '-' + df['Month'].astype(str).str.zfill(2) + '-01',
                errors='coerce'
            )


            for col in ['STOIM', 'STOIM_NAC_VAL', 'NETTO', 'KOL']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            stoim_sum = df['STOIM'].fillna(0).sum() if 'STOIM' in df.columns else 0
            kol_sum = df['KOL'].fillna(0).sum() if 'KOL' in df.columns else 0
            if stoim_sum == 0 and kol_sum == 0:
                logger.info(f"  → пропуск {file_path.name}: STOIM и KOL нулевые (скелет MEIDB)")
                continue

            # MEIDB: сначала приводим к тыс. USD (млн → ×1000 по сумме файла).
            if 'STOIM' in df.columns:
                multiplier = infer_india_stoim_multiplier(df['STOIM'])
                if multiplier != 1.0:
                    df['STOIM'] = df['STOIM'] * multiplier
                # CN/TR в unified parquet и на Superset — STOIM в USD, не в тыс.
                df['STOIM'] = df['STOIM'] * 1000
                # Громкая проверка результата: если множитель выбран неверно (мисскейл
                # 1000x), медианная удельная цена вылетит за коридор и файл будет отвергнут
                # здесь (ниже except -> ERROR + skip), а не молча испортит данные. На верно
                # масштабированных файлах это no-op.
                netto_series = (
                    df['NETTO'] if 'NETTO' in df.columns
                    else pd.Series(index=df.index, dtype=float)
                )
                median_unit_value = assert_india_stoim_unit_value_sane(
                    df['STOIM'], netto_series, label=file_path.name
                )
                logger.info(
                    f"     MEIDB→USD: x{multiplier:g}x1000 (raw sum={stoim_sum:,.2f}; "
                    f"median {median_unit_value:,.3f} USD/kg)"
                )

            # Map units through the common EDIZM normalization layer.
            if 'EDIZM' in df.columns:
                original_edizm = df['EDIZM'].astype(str).str.strip().replace({'nan': '?', 'None': '?'})
            else:
                original_edizm = pd.Series('?', index=df.index)

            edizm_records = resolve_edizm_records(original_edizm, common_edizm_map)
            df['EDIZM_ISO'] = edizm_records.map(
                lambda record: record.get('KOD') if isinstance(record, dict) else None
            )
            df['EDIZM'] = edizm_records.map(
                lambda record: record.get('NAME') if isinstance(record, dict) else None
            ).fillna(original_edizm)

            df_final = df[[
                'NAPR', 'PERIOD', 'STRANA', 'TNVED',
                'EDIZM', 'EDIZM_ISO', 'STOIM', 'NETTO',
                'KOL', 'TNVED4', 'TNVED6', 'TNVED2'
            ]].copy()

            period_label = df_final['PERIOD'].iloc[0].strftime('%Y-%m') if len(df_final) else "?"
            total_stoim = df_final['STOIM'].sum()
            logger.info(
                f"     STOIM сумма за {period_label}: {total_stoim:,.0f} USD "
                f"({total_stoim / 1e9:.3f} млрд)"
            )

            dfs.append(df_final)

        except Exception as e:
            logger.error(f"Ошибка при обработке {file_path.name}: {e}")

    if not dfs:
        logger.error("Не удалось обработать ни один файл.")
        return

    final_df = finalize_country_output(
        pd.concat(dfs, ignore_index=True),
        country_code=processor_input.country_code,
    )
    assert_country_output_contract(final_df, expected_strana=processor_input.country_code)

    # Сводка по месяцам — для проверки размерности (резкий скачок/провал = возможная ошибка масштаба)
    stoim_by_month = final_df.groupby(final_df['PERIOD'].dt.to_period('M'))['STOIM'].sum()
    logger.info("STOIM по месяцам (USD), для проверки размерности:")
    for period, total in stoim_by_month.items():
        logger.info(f"  {period}: {total:,.0f} ({total / 1e9:.3f} млрд)")

    save_country_output(final_df, processor_input.output_file, logger=logger)

def main():
    """
    Точка входа в скрипт. Определяет пути к данным и запускает
    процесс обработки и слияния.
    """
    project_root = get_project_root()

    raw_data_dir = project_root / 'data_raw' / 'india_new'
    output_file = project_root / 'data_processed' / 'in_full.parquet'
    edizm_file = project_root / 'metadata' / 'edizm.csv'

    process_and_merge_india_data(raw_data_dir, output_file, edizm_file)


if __name__ == "__main__":
    main()
