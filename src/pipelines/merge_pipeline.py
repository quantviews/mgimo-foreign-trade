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
7. Optionally loads nowcast from data_processed/nowcast/nowcast.parquet (TYPE=pred; on by default, disable with --no-nowcast). Any pred row whose (PERIOD, STRANA, TNVED, NAPR) key already exists in factual rows (national/comtrade) is dropped so nowcast fills only gaps.
8. Optionally loads fizob parquet files into fizob_index (on by default, disable with --no-fizob).
9. Saves the merged dataset to DuckDB format in db/unified_trade_data.duckdb

The implementation lives in ``src/core`` modules; this file orchestrates the
pipeline stages (discover -> load -> append -> merge -> save).
"""

import argparse
import logging
from pathlib import Path
from typing import Dict, List

import duckdb
import pandas as pd

from core.comtrade import load_and_transform_comtrade
from core.duckdb_writer import save_to_duckdb
from core.edizm import load_common_edizm_mapping
from core.fizob import transform_fizob_to_unified
from core.normalization_rules import (
    add_tnved_columns,
    apply_special_edizm_cases,
    standardize_edizm_columns,
)
from core.reference_tables import save_reference_tables
from core.schema import load_and_validate_file, smoke_check_merged_dataset
from core.tnved import generate_derived_columns
from pipelines.nowcast_ingest import (
    append_nowcast_data,
    drop_nowcast_rows_superseded_by_facts,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_merge_args(argv: List[str] = None):
    """Parse CLI arguments for the merge pipeline."""
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
        '--output-db-path',
        type=str,
        default=None,
        help=(
            "Path to the output DuckDB file. Relative paths are resolved from "
            "the project root. Default: db/unified_trade_data.duckdb."
        ),
    )
    parser.add_argument(
        '--no-nowcast',
        dest='include_nowcast',
        action='store_false',
        help="Do not load nowcast from data_processed/nowcast/nowcast.parquet (TYPE=pred rows).",
    )
    parser.add_argument(
        '--no-fizob',
        dest='include_fizob',
        action='store_false',
        help="Do not load fizob_*.parquet files into the fizob_index table.",
    )
    parser.set_defaults(include_nowcast=True, include_fizob=True)
    return parser.parse_args(argv)


def resolve_merge_paths(project_root: Path = None, output_db_path: str = None) -> Dict[str, Path]:
    """Resolve project paths used by the merge pipeline."""
    if project_root is None:
        project_root = Path(__file__).resolve().parents[2]
    data_processed_dir = project_root / "data_processed"
    db_dir = project_root / "db"
    db_dir.mkdir(exist_ok=True)

    if output_db_path is None:
        resolved_output_db_path = db_dir / "unified_trade_data.duckdb"
    else:
        resolved_output_db_path = Path(output_db_path)
        if not resolved_output_db_path.is_absolute():
            resolved_output_db_path = project_root / resolved_output_db_path

    return {
        "project_root": project_root,
        "data_processed_dir": data_processed_dir,
        "db_dir": db_dir,
        "output_db_path": resolved_output_db_path,
        "comtrade_db_path": db_dir / "comtrade.db",
        "nowcast_path": data_processed_dir / "nowcast" / "nowcast.parquet",
    }


def discover_processed_files(data_processed_dir: Path):
    """Find regular and fizob parquet files in data_processed."""
    parquet_files = list(data_processed_dir.glob("*.parquet"))
    logger.info(f"Found {len(parquet_files)} parquet files: {[f.name for f in parquet_files]}")

    fizob_files = [f for f in parquet_files if f.name.startswith('fizob')]
    regular_files = [f for f in parquet_files if not f.name.startswith('fizob')]

    logger.info(f"Found {len(fizob_files)} fizob files: {[f.name for f in fizob_files]}")
    logger.info(f"Found {len(regular_files)} regular data files: {[f.name for f in regular_files]}")
    return regular_files, fizob_files


def load_national_datasets(
    regular_files: List[Path],
    excluded_countries_upper: List[str],
    start_year: int = None,
) -> Dict[str, pd.DataFrame]:
    """Load, validate and normalize processed national parquet datasets."""
    national_datasets = {}
    if not regular_files:
        logger.warning("No regular national parquet files found in data_processed directory.")
        return national_datasets

    for file_path in regular_files:
        country_code = file_path.stem.replace('_full', '').upper()
        if country_code in excluded_countries_upper:
            logger.info(f"Skipping {file_path.name} as per --exclude-countries argument.")
            continue

        df = load_and_validate_file(file_path, start_year=start_year)
        if df is not None:
            df_processed = generate_derived_columns(df)
            if 'STRANA' in df_processed.columns:
                df_processed['STRANA'] = df_processed['STRANA'].str.upper()
            national_datasets[country_code.lower()] = df_processed

    return national_datasets


def load_fizob_index_rows(fizob_files: List[Path], start_year: int = None) -> List[pd.DataFrame]:
    """Load fizob parquet files and transform them to unified fizob_index rows."""
    fizob_index_rows = []
    if not fizob_files:
        return fizob_index_rows

    logger.info("Loading fizob files for unified fizob_index table...")
    for file_path in fizob_files:
        file_stem = file_path.stem
        df = load_and_validate_file(file_path, start_year=start_year)
        if df is not None:
            df_processed = generate_derived_columns(df)
            if 'STRANA' in df_processed.columns:
                df_processed['STRANA'] = df_processed['STRANA'].str.upper()
            df_unified = transform_fizob_to_unified(df_processed, file_stem)
            if not df_unified.empty:
                fizob_index_rows.append(df_unified)
                logger.info(f"Loaded {file_stem}: {len(df_unified)} rows -> fizob_index")

    return fizob_index_rows


def append_national_data(
    all_dataframes: List[pd.DataFrame],
    national_datasets: Dict[str, pd.DataFrame],
) -> List[str]:
    """Append national datasets and return covered ISO country codes."""
    national_countries_iso = []
    for source_name, df in national_datasets.items():
        df['SOURCE'] = 'national'
        if 'TYPE' not in df.columns:
            df['TYPE'] = 'fact'
        else:
            df['TYPE'] = df['TYPE'].fillna('fact')
        all_dataframes.append(df)

        if 'STRANA' in df.columns and not df.empty:
            unique_countries = df['STRANA'].dropna().unique()
            for country in unique_countries:
                country_upper = country.upper()
                if country_upper not in national_countries_iso:
                    national_countries_iso.append(country_upper)

    return national_countries_iso


def append_comtrade_data(
    all_dataframes: List[pd.DataFrame],
    *,
    include_comtrade: bool,
    comtrade_db_path: Path,
    project_root: Path,
    national_countries_iso: List[str],
    excluded_countries_upper: List[str],
    start_year: int = None,
) -> None:
    """Optionally append transformed Comtrade data."""
    if not include_comtrade:
        return

    if not comtrade_db_path.exists():
        logger.error(f"Comtrade database not found at {comtrade_db_path}. Cannot include Comtrade data.")
        return

    countries_to_exclude_from_comtrade = list(set(national_countries_iso + excluded_countries_upper))
    logger.info(f"Excluding countries from Comtrade data to avoid duplicates: {countries_to_exclude_from_comtrade}")

    comtrade_df = load_and_transform_comtrade(
        comtrade_db_path,
        project_root,
        exclude_countries=countries_to_exclude_from_comtrade,
        start_year=start_year
    )
    if comtrade_df.empty:
        return

    initial_comtrade_rows = len(comtrade_df)
    indices_to_drop = comtrade_df[comtrade_df['STRANA'].isin(national_countries_iso)].index
    comtrade_df.drop(indices_to_drop, inplace=True)
    filtered_rows = initial_comtrade_rows - len(comtrade_df)
    if filtered_rows > 0:
        logger.info(f"Filtered {filtered_rows:,} duplicate rows from Comtrade data that matched national countries.")

    comtrade_df['SOURCE'] = 'comtrade'
    comtrade_df['TYPE'] = 'fact'
    all_dataframes.append(comtrade_df)


def build_merged_dataframe(
    all_dataframes: List[pd.DataFrame],
    *,
    excluded_countries_upper: List[str],
    project_root: Path,
) -> pd.DataFrame:
    """Merge all sources and apply final shared normalization rules."""
    if not all_dataframes:
        logger.error("No data available to merge.")
        return pd.DataFrame()

    merged_df = pd.concat(all_dataframes, ignore_index=True)

    if excluded_countries_upper:
        initial_rows = len(merged_df)
        indices_to_drop = merged_df[merged_df['STRANA'].isin(excluded_countries_upper)].index
        merged_df.drop(indices_to_drop, inplace=True)
        excluded_rows = initial_rows - len(merged_df)
        if excluded_rows > 0:
            logger.info(f"Excluded {excluded_rows:,} rows for countries: {excluded_countries_upper}")

    merged_df = merged_df.sort_values(['PERIOD', 'STRANA', 'TNVED'])

    initial_rows = len(merged_df)
    merged_df.dropna(subset=['NAPR'], inplace=True)
    null_napr_rows = initial_rows - len(merged_df)
    if null_napr_rows > 0:
        logger.info(f"Removed {null_napr_rows:,} rows with NULL NAPR values")

    merged_df = drop_nowcast_rows_superseded_by_facts(merged_df, logger)

    logger.info("Standardizing EDIZM column...")
    common_edizm_map = load_common_edizm_mapping(project_root)
    if common_edizm_map:
        merged_df = standardize_edizm_columns(merged_df, common_edizm_map, logger)
    else:
        logger.error("Could not standardize EDIZM values due to mapping load failure.")

    merged_df = add_tnved_columns(merged_df)
    return apply_special_edizm_cases(merged_df, logger)


def save_fizob_index(fizob_index_rows: List[pd.DataFrame], output_db_path: Path) -> None:
    """Save unified fizob_index table and computed view."""
    if not fizob_index_rows:
        return

    logger.info("Saving unified fizob_index table...")
    fizob_index_df = pd.concat(fizob_index_rows, ignore_index=True)
    conn = duckdb.connect(str(output_db_path))
    try:
        chunk_size = 100000
        first_chunk = fizob_index_df.iloc[:chunk_size]
        conn.register('fizob_chunk_df', first_chunk)
        conn.execute("""
            CREATE OR REPLACE TABLE fizob_index AS
            SELECT STRANA, NAPR, CAST(PERIOD AS DATE) AS PERIOD, tn_level, tn_code, fizob, fizob_bp
            FROM fizob_chunk_df
        """)
        conn.unregister('fizob_chunk_df')
        logger.info(f"  ... created fizob_index and inserted first {len(first_chunk):,} rows")

        for i in range(chunk_size, len(fizob_index_df), chunk_size):
            chunk = fizob_index_df.iloc[i:i + chunk_size]
            conn.register('fizob_chunk_df', chunk)
            conn.execute("""
                INSERT INTO fizob_index
                SELECT STRANA, NAPR, CAST(PERIOD AS DATE) AS PERIOD, tn_level, tn_code, fizob, fizob_bp
                FROM fizob_chunk_df
            """)
            conn.unregister('fizob_chunk_df')
            logger.info(f"  ... inserted {i + len(chunk):,} / {len(fizob_index_df):,} rows")

        result = conn.execute("SELECT COUNT(*) FROM fizob_index").fetchone()
        logger.info(f"  ... saved {result[0]:,} rows to fizob_index")

        conn.execute("""
            CREATE OR REPLACE VIEW fizob_index_v AS
            SELECT *,
                   CASE WHEN fizob_bp = 0 THEN NULL ELSE fizob / fizob_bp END AS idx
            FROM fizob_index
        """)
        logger.info("  ... created view fizob_index_v with computed idx column")
    except Exception as e:
        logger.error(f"Failed to save fizob_index: {e}")
        raise
    finally:
        conn.close()


def create_reference_tables(output_db_path: Path, project_root: Path) -> None:
    """Create DuckDB reference tables and convenience views."""
    conn = duckdb.connect(str(output_db_path))
    try:
        save_reference_tables(conn, project_root)
    except Exception as e:
        logger.error(f"Failed to create reference tables: {e}")
        raise
    finally:
        conn.close()


def log_merge_summary(merged_df: pd.DataFrame) -> None:
    """Log final merge and fact/pred coverage summary."""
    logger.info("=== MERGE SUMMARY ===")
    logger.info(f"Total rows: {len(merged_df)}")
    logger.info(f"Unique countries: {merged_df['STRANA'].nunique()}")
    logger.info(f"Date range: {merged_df['PERIOD'].min()} to {merged_df['PERIOD'].max()}")

    logger.info("Rows by source:")
    source_counts = merged_df['SOURCE'].value_counts()
    for source, count in source_counts.items():
        logger.info(f"  {source}: {count:,} rows")

    logger.info("=== SANITY CHECK: FACT VS PRED ===")
    if 'TYPE' in merged_df.columns:
        merged_df['TYPE'] = merged_df['TYPE'].fillna('fact')
        type_counts = merged_df['TYPE'].value_counts(dropna=False)
        total_rows = len(merged_df)
        for type_value, count in type_counts.items():
            share = (count / total_rows * 100) if total_rows > 0 else 0
            logger.info(f"  TYPE={type_value}: {count:,} rows ({share:.2f}%)")

        pred_df = merged_df[merged_df['TYPE'] == 'pred'].copy()
        if pred_df.empty:
            logger.info("  No TYPE='pred' rows found in merged dataset.")
        else:
            logger.info(f"  Pred date range: {pred_df['PERIOD'].min()} to {pred_df['PERIOD'].max()}")
            pred_month_counts = (
                pred_df
                .groupby(pred_df['PERIOD'].dt.to_period('M'))
                .size()
                .sort_index()
            )
            logger.info("  Pred rows by month:")
            for period_month, count in pred_month_counts.items():
                logger.info(f"    {period_month}: {count:,}")

            pred_country_counts = pred_df.groupby('STRANA').size().sort_values(ascending=False)
            logger.info("  Pred rows by country:")
            for country, count in pred_country_counts.items():
                logger.info(f"    {country}: {count:,}")

            pred_country_month = (
                pred_df.assign(PERIOD_MONTH=pred_df['PERIOD'].dt.to_period('M').astype(str))
                .groupby(['STRANA', 'PERIOD_MONTH'])
                .size()
                .reset_index(name='count')
                .sort_values(['STRANA', 'PERIOD_MONTH'])
            )
            logger.info("  Pred coverage by country and month:")
            for _, row in pred_country_month.iterrows():
                logger.info(f"    {row['STRANA']} | {row['PERIOD_MONTH']}: {row['count']:,}")
    else:
        logger.warning("TYPE column not found: sanity-check for fact/pred skipped.")

    logger.info("Rows by country:")
    logger.info(str(merged_df.groupby('SOURCE')['STRANA'].value_counts()))

    logger.info("EDIZM counts by country:")
    edizm_counts = merged_df.groupby(['STRANA', 'EDIZM']).size().reset_index(name='count')
    edizm_counts = edizm_counts.sort_values(['STRANA', 'count'], ascending=[True, False])
    for strana, group in edizm_counts.groupby('STRANA'):
        logger.info(f"  Country: {strana}")
        for _, row in group.head(5).iterrows():
            logger.info(f"    - {row['EDIZM']}: {row['count']:,} rows")


def run_merge_pipeline(args, paths: Dict[str, Path]) -> None:
    """Run the merge pipeline stages in order."""
    logger.info("Starting data merging process...")

    excluded_countries_upper = [c.upper() for c in args.exclude_countries]
    regular_files, fizob_files = discover_processed_files(paths["data_processed_dir"])
    national_datasets = load_national_datasets(
        regular_files,
        excluded_countries_upper,
        start_year=args.start_year,
    )
    if args.include_fizob:
        fizob_index_rows = load_fizob_index_rows(fizob_files, start_year=args.start_year)
    else:
        logger.info("Fizob disabled (--no-fizob); not loading fizob_*.parquet into fizob_index.")
        fizob_index_rows = []

    all_dataframes = []
    national_countries_iso = append_national_data(all_dataframes, national_datasets)
    append_comtrade_data(
        all_dataframes,
        include_comtrade=args.include_comtrade,
        comtrade_db_path=paths["comtrade_db_path"],
        project_root=paths["project_root"],
        national_countries_iso=national_countries_iso,
        excluded_countries_upper=excluded_countries_upper,
        start_year=args.start_year,
    )
    append_nowcast_data(
        all_dataframes,
        include_nowcast=args.include_nowcast,
        nowcast_path=paths["nowcast_path"],
        excluded_countries_upper=excluded_countries_upper,
        start_year=args.start_year,
    )

    merged_df = build_merged_dataframe(
        all_dataframes,
        excluded_countries_upper=excluded_countries_upper,
        project_root=paths["project_root"],
    )
    if merged_df.empty:
        return

    logger.info("Reference tables (country names, TNVED names) will be created as separate tables in the database.")
    if not smoke_check_merged_dataset(merged_df):
        logger.error("Aborting save: smoke checks failed. The existing DuckDB was NOT modified.")
        return

    save_to_duckdb(merged_df, paths["output_db_path"])
    save_fizob_index(fizob_index_rows, paths["output_db_path"])
    create_reference_tables(paths["output_db_path"], paths["project_root"])

    logger.info("Data merge completed. To process outliers, run: python src/outlier_detection.py")
    log_merge_summary(merged_df)


def main(argv: List[str] = None):
    """CLI orchestration layer for the merge pipeline."""
    args = parse_merge_args(argv)
    paths = resolve_merge_paths(output_db_path=args.output_db_path)
    run_merge_pipeline(args, paths)

if __name__ == "__main__":
    main()
