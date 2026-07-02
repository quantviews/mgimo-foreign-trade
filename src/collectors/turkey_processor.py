"""
Обработка данных из Excel файлов, организованных по годам и месяцам.

Структура папок: path/{YYYY}/{YYYY}-{MM}.xlsx
Пример: /data/2019/2019-01.xlsx, /data/2019/2019-02.xlsx и т.д.

Основные функции:
  - load_and_process_data(): загрузить и обработать все файлы
  - save_dataset(): сохранить датасет в парquet файл
  - find_data_files(): найти все файлы по структуре
  - normalize(): нормализовать данные из Excel

Использование:
  from data_processor import load_and_process_data, save_dataset

  df, stats = load_and_process_data("/path/to/data")
  save_dataset(df, "/output/dataset.parquet")
"""

import logging
from pathlib import Path
from datetime import datetime
from typing import Tuple, Dict, List, Optional
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Словарь единиц измерения
iso_dict = {
    "KG/ÇİFT": {"KOD": "715", "NAME": "ПАРА"},
    "KG/METR E": {"KOD": "006", "NAME": "МЕТР"},
    "KG/1000A DET": {"KOD": "798", "NAME": "ТЫСЯЧА ШТУК"},
    "KG/KG P2O5": {"KOD": "865", "NAME": "КИЛОГРАММ ПЯТИОКИСИ ФОСФОРА"},
    "KG/ADET": {"KOD": "796", "NAME": "ШТУКА"},
    "KG/M3": {"KOD": "113", "NAME": "КУБИЧЕСКИЙ МЕТР"},
    "KG/KG K2O": {"KOD": "852", "NAME": "КИЛОГРАММ ОКСИДА КАЛИЯ"},
    "KG/KG MET.AM.": {"KOD": None, "NAME": "КИЛОГРАММ МЕТИЛАМИНА"},
    "KG/1000LI TRE": {"KOD": "130", "NAME": "1000 ЛИТРОВ"},
    "KG/CE-EL": {"KOD": "745", "NAME": "ЭЛЕМЕНТ"},
    "KG/LİTRE": {"KOD": "112", "NAME": "ЛИТР"},
    "KG/BAŞ": {"KOD": "836", "NAME": "ГОЛОВА"},
    "KG/KARA T": {"KOD": "162", "NAME": "МЕТРИЧЕСКИЙ КАРАТ(1КАРАТ=2*10(-4)КГ"},
    "KG/100AD ET": {"KOD": "797", "NAME": "СТО ШТУК"},
    "KG/KG N": {"KOD": "861", "NAME": "КИЛОГРАММ АЗОТА"},
    "KG/M2": {"KOD": "055", "NAME": "КВАДРАТНЫЙ МЕТР"},
    "KG/LT- ALK%100": {"KOD": "831", "NAME": "ЛИТР ЧИСТОГО (100%) СПИРТА"},
    "KG/KG H2O2": {"KOD": "841", "NAME": "КИЛОГРАММ ПЕРОКСИДА ВОДОРОДА"},
    "KG/GRAM": {"KOD": "163", "NAME": "ГРАММ"},
    "KG/KG U": {"KOD": "867", "NAME": "КИЛОГРАММ УРАНА"},
    "KG/1000M 3": {"KOD": "114", "NAME": "1000 КУБИЧЕСКИХ МЕТРОВ"},
    "KG/GI F/S": {"KOD": None, "NAME": "gi F/S"},
    "KG/CT-L": {"KOD": None, "NAME": "CT-L"},
    "G.T/ADET": {"KOD": "796", "NAME": "ШТУКА"},
    "KG/KG NET EDA": {"KOD": None, "NAME": "KG NET EDA"},
    "KG/KG %90 SDT": {"KOD": "845", "NAME": "КИЛОГРАММ СУХОГО НА 90 % ВЕЩЕСТВА"},
    "KG/KG KOH": {"KOD": "859", "NAME": "КИЛОГРАММ ГИДРОКСИДА КАЛИЯ"},
    "KG/KG NAOH": {"KOD": "863", "NAME": "КИЛОГРАММ ГИДРОКСИДА НАТРИЯ"},
}


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """
    Нормализация данных из Excel файла.

    Args:
        df: DataFrame из Excel файла

    Returns:
        Обработанный DataFrame с импортом и экспортом
    """
    # Пропускаем пустые строки в начале файла и ищем заголовки
    header_row = None
    for idx, row in df.iterrows():
        if "Yıl" in str(row.values) or "Год" in str(row.values) or "Year" in str(row.values):
            header_row = idx
            break

    # Если заголовки не найдены, пытаемся использовать стандартный формат
    if header_row is None:
        # Пытаемся получить год/месяц из первого ряда данных
        year = f"{int(df.iloc[0, 0])}"
        month = f"{int(df.iloc[0, 1]):02d}"
        df = df[pd.to_numeric(df["GTİP"], errors="coerce").notna()]
        df = df[df.columns[4:]]
    else:
        # Используем данные из строки после заголовков
        data_row_idx = header_row + 1
        year = f"{int(df.iloc[data_row_idx, 0])}"
        month = f"{int(df.iloc[data_row_idx, 1]):02d}"

        # Берем данные начиная со строки заголовков
        df = df.iloc[header_row:].reset_index(drop=True)
        df.columns = df.iloc[0]
        df = df[1:].reset_index(drop=True)
        df = df[pd.to_numeric(df["GTİP"], errors="coerce").notna()]

    df["PERIOD"] = pd.to_datetime(f"{year}-{month}-01")
    df["STRANA"] = "TR"
    df["TNVED"] = df["GTİP"].str[:8]
    df["TNVED6"] = df["GTİP"].str[:6]
    df["TNVED4"] = df["GTİP"].str[:4]
    df["TNVED2"] = df["GTİP"].str[:2]
    df["EDIZM_ISO"] = df["Ölçü"].map(lambda x: iso_dict.get(x, {}).get("KOD"))
    df = df[df.columns[2:]]

    df_import = df[
        df["İhracat USD"].notnull() &
        (df["İhracat USD"] != 0.0)
    ].copy()

    df_export = df[
        df["İthalat USD"].notnull() &
        (df["İthalat USD"] != 0.0)
    ].copy()

    df_import["NAPR"] = "ИМ"
    df_import = df_import.rename(
        columns={
            "İhracat USD": "STOIM",
            "İhracat Miktar 1 (kilogram)": "NETTO",
            "İhracat Miktar 2": "KOL",
            "Ölçü": "EDIZM",
        }
    )

    df_export["NAPR"] = "ЭК"
    df_export = df_export.rename(
        columns={
            "İthalat USD": "STOIM",
            "İthalat Miktar 1 (kilogram)": "NETTO",
            "İthalat Miktar 2": "KOL",
            "Ölçü": "EDIZM",
        }
    )

    cols = [
        "NAPR",
        "PERIOD",
        "STRANA",
        "TNVED",
        "EDIZM",
        "EDIZM_ISO",
        "KOL",
        "TNVED4",
        "TNVED6",
        "TNVED2",
        "NETTO",
        "STOIM",
    ]
    df_import = df_import[cols]
    df_export = df_export[cols]

    return pd.concat([df_import, df_export], ignore_index=True).sort_values(by="TNVED", ascending=True)




def get_expected_months(
    start_year: int = 2019,
    end_year: Optional[int] = None
) -> List[Tuple[int, int]]:
    """
    Получить список ожидаемых месяцев от start_year до end_year.

    Args:
        start_year: Начальный год (по умолчанию 2019)
        end_year: Конечный год (по умолчанию текущий год)

    Returns:
        Список кортежей (год, месяц)
    """
    if end_year is None:
        end_year = datetime.now().year

    expected = []
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            # Пропускаем будущие месяцы текущего года
            if year == end_year and month > datetime.now().month:
                break
            expected.append((year, month))

    return expected


def find_data_files(
    base_path: str,
    start_year: int = 2019,
    end_year: Optional[int] = None
) -> Tuple[Dict[Tuple[int, int], Path], List[Tuple[int, int]]]:
    """
    Найти все файлы данных в формате {YYYY}/{YYYY}-{MM}.xlsx.

    Args:
        base_path: Базовый путь к папкам с данными
        start_year: Начальный год (по умолчанию 2019)
        end_year: Конечный год (по умолчанию текущий год)

    Returns:
        Кортеж (найденные_файлы, отсутствующие_месяцы)
        найденные_файлы: словарь {(год, месяц): Path}
        отсутствующие_месяцы: список [(год, месяц), ...]
    """
    if end_year is None:
        end_year = datetime.now().year

    base_path = Path(base_path)
    found_files = {}
    missing_months = []

    expected_months = get_expected_months(start_year, end_year)

    for year, month in expected_months:
        year_path = base_path / str(year)
        file_path = year_path / f"{year}-{month:02d}.xlsx"

        if file_path.exists():
            found_files[(year, month)] = file_path
        else:
            missing_months.append((year, month))

    return found_files, missing_months


def load_and_process_data(
    base_path: str,
    start_year: int = 2019,
    end_year: Optional[int] = None,
    verbose: bool = True,
    output_path: Optional[str] = None
) -> Tuple[Optional[pd.DataFrame], Dict]:
    """
    Загрузить и обработать все файлы данных.

    Args:
        base_path: Базовый путь к папкам с данными
        start_year: Начальный год (по умолчанию 2019)
        end_year: Конечный год (по умолчанию текущий год)
        verbose: Выводить подробные логи (по умолчанию True)
        output_path: Путь для сохранения в парquet формате (опционально)

    Returns:
        Кортеж (результирующий_датасет, статистика)
        результирующий_датасет: объединённый DataFrame или None если ошибка
        статистика: словарь с информацией об обработке

    Пример:
        df, stats = load_and_process_data(
            "/data/raw",
            output_path="/data/processed/dataset.parquet"
        )
    """
    if end_year is None:
        end_year = datetime.now().year

    logger.info(f"Ищу файлы в {base_path} с {start_year} по {end_year} год(ы)...")
    found_files, missing_months = find_data_files(base_path, start_year, end_year)

    # Вывести информацию об отсутствующих месяцах
    if missing_months:
        if verbose:
            logger.warning(f"Отсутствующие месяцы: {len(missing_months)}")
            for year, month in missing_months[:10]:  # Показать первые 10
                logger.warning(f"  - {year}-{month:02d}")
            if len(missing_months) > 10:
                logger.warning(f"  ... и ещё {len(missing_months) - 10}")
    else:
        if verbose:
            logger.info("Все ожидаемые месяцы найдены")

    # Загрузить и обработать файлы
    dfs = []
    failed_files = []

    logger.info(f"Обработка {len(found_files)} файлов...")

    for (year, month), file_path in found_files.items():
        try:
            df = pd.read_excel(file_path)
            df_processed = normalize(df)
            dfs.append(df_processed)

            if verbose:
                logger.debug(f"Обработан {year}-{month:02d}: {len(df_processed)} строк")

        except Exception as e:
            error_msg = str(e)
            failed_files.append({
                'file': f"{year}-{month:02d}",
                'path': str(file_path),
                'error': error_msg
            })
            logger.error(f"Ошибка при обработке {file_path}: {error_msg}")

    # Вывести информацию об ошибках
    if failed_files and verbose:
        logger.error(f"Не удалось обработать {len(failed_files)} файлов:")
        for item in failed_files:
            logger.error(f"  - {item['file']}: {item['error']}")

    # Объединить всё в один датасет
    result_df = None
    if dfs:
        result_df = pd.concat(dfs, ignore_index=True)
        if verbose:
            logger.info(f"Успешно загружено и обработано {len(dfs)} файлов")
            logger.info(f"Итоговый датасет: {len(result_df)} строк")
    else:
        logger.error("Не удалось загрузить ни один файл")

    # Сохранить датасет если указан output_path
    if output_path and result_df is not None:
        logger.info(f"Сохраняю датасет в {output_path}...")
        save_dataset(result_df, output_path)

    # Подготовить статистику
    stats = {
        'total_files': len(found_files),
        'processed_files': len(dfs),
        'failed_files': failed_files,
        'missing_months': missing_months,
        'total_rows': len(result_df) if result_df is not None else 0,
        'output_path': output_path if output_path and result_df is not None else None,
    }

    return result_df, stats


def save_dataset(
    df: pd.DataFrame,
    output_path: str,
    compression: str = "snappy"
) -> bool:
    """
    Сохранить датасет в парquet файл.

    Парquet формат обеспечивает:
    - Компактное хранение данных (~50-80% от исходного размера)
    - Быстрое чтение и фильтрацию
    - Сохранение типов данных
    - Быстрое сжатие (snappy, gzip, brotli)

    Args:
        df: DataFrame для сохранения
        output_path: Путь для сохранения файла (с расширением .parquet)
        compression: Алгоритм сжатия ('snappy', 'gzip', 'brotli' или None)

    Returns:
        True если успешно, False если ошибка

    Пример:
        success = save_dataset(df, "/data/output/dataset.parquet")
    """
    if df is None or len(df) == 0:
        logger.error("DataFrame пуст, нечего сохранять")
        return False

    try:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        df.to_parquet(output_path, compression=compression, index=False)
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        logger.info(f"Датасет сохранен в {output_path}")
        logger.info(f"Размер файла: {file_size_mb:.2f} MB")
        logger.info(f"Строк: {len(df):,}, Столбцов: {len(df.columns)}")
        return True

    except Exception as e:
        logger.error(f"Ошибка при сохранении файла {output_path}: {e}")
        return False


def load_dataset(file_path: str) -> Optional[pd.DataFrame]:
    """
    Загрузить датасет из парquet файла.

    Args:
        file_path: Путь к парquet файлу

    Returns:
        Загруженный DataFrame или None при ошибке

    Пример:
        df = load_dataset("/data/output/dataset.parquet")
    """
    try:
        file_path = Path(file_path)

        if not file_path.exists():
            logger.error(f"Файл не найден: {file_path}")
            return None

        if file_path.suffix.lower() == '.parquet':
            df = pd.read_parquet(file_path)
            logger.info(f"Датасет загружен из Parquet: {file_path}")
        elif file_path.suffix.lower() == '.csv':
            df = pd.read_csv(file_path)
            logger.info(f"Датасет загружен из CSV: {file_path}")
        else:
            logger.error(f"Неподдерживаемый формат: {file_path.suffix}")
            return None

        logger.info(f"Размер: {len(df):,} строк, {len(df.columns)} столбцов")
        return df

    except Exception as e:
        logger.error(f"Ошибка при загрузке датасета: {e}")
        return None


def print_statistics(stats: Dict) -> None:
    """
    Вывести красивую статистику обработки.

    Args:
        stats: Словарь статистики от load_and_process_data
    """
    print("\n" + "="*70)
    print("СТАТИСТИКА ОБРАБОТКИ ДАННЫХ")
    print("="*70)

    print(f"\nФайлы:")
    print(f"  Найдено файлов:        {stats['total_files']}")
    print(f"  Успешно обработано:    {stats['processed_files']}")
    print(f"  Ошибок обработки:      {len(stats['failed_files'])}")
    print(f"  Отсутствующих:         {len(stats['missing_months'])}")

    print(f"\nДанные:")
    print(f"  Всего строк в датасете: {stats['total_rows']:,}")

    if stats.get('output_path'):
        print(f"\nВывод:")
        print(f"  Сохранен в: {stats['output_path']}")

    if stats['missing_months']:
        print(f"\nОтсутствующие месяцы ({len(stats['missing_months'])}):")
        # Сгруппировать по годам
        by_year = {}
        for year, month in stats['missing_months']:
            if year not in by_year:
                by_year[year] = []
            by_year[year].append(month)

        for year in sorted(by_year.keys()):
            months = sorted(by_year[year])
            months_str = ", ".join(f"{m:02d}" for m in months)
            print(f"  {year}: {months_str}")

    if stats['failed_files']:
        print(f"\nФайлы с ошибками ({len(stats['failed_files'])}):")
        for item in stats['failed_files'][:10]:  # Показать первые 10
            print(f"  - {item['file']}: {item['error'][:60]}...")
        if len(stats['failed_files']) > 10:
            print(f"  ... и ещё {len(stats['failed_files']) - 10}")

    print("\n" + "="*70 + "\n")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Использование:")
        print("  python data_processor.py <path_to_data> [output_path] [start_year] [end_year]")
        print("\nПараметры:")
        print("  path_to_data - путь к папке с файлами Excel (структура: {YYYY}/{YYYY}-{MM}.xlsx)")
        print("  output_path  - путь для сохранения результата в parquet (опционально)")
        print("  start_year   - начальный год (по умолчанию 2019)")
        print("  end_year     - конечный год (по умолчанию текущий)")
        print("\nПримеры:")
        print("  python data_processor.py /path/to/data")
        print("  python data_processor.py /path/to/data dataset.parquet")
        print("  python data_processor.py /path/to/data dataset.parquet 2019 2024")
        sys.exit(1)

    base_path = sys.argv[1]

    # Определяем output_path - если содержит '/', '\\' или '.parquet', то это путь
    output_path = None
    start_year = 2019
    end_year = None

    if len(sys.argv) > 2:
        arg2 = sys.argv[2]
        # Проверяем является ли это путем к файлу
        if '/' in arg2 or '\\' in arg2 or arg2.endswith('.parquet'):
            output_path = arg2
            if len(sys.argv) > 3:
                start_year = int(sys.argv[3])
            if len(sys.argv) > 4:
                end_year = int(sys.argv[4])
        else:
            # Это год
            try:
                start_year = int(arg2)
                if len(sys.argv) > 3:
                    arg3 = sys.argv[3]
                    if '/' in arg3 or '\\' in arg3 or arg3.endswith('.parquet'):
                        output_path = arg3
                    else:
                        end_year = int(arg3)
                        if len(sys.argv) > 4:
                            output_path = sys.argv[4]
                if len(sys.argv) > 4 and not output_path:
                    arg4 = sys.argv[4]
                    if '/' in arg4 or '\\' in arg4 or arg4.endswith('.parquet'):
                        output_path = arg4
                    else:
                        end_year = int(arg4)
            except ValueError:
                # Не год, считаем что это путь
                output_path = arg2
                if len(sys.argv) > 3:
                    start_year = int(sys.argv[3])
                if len(sys.argv) > 4:
                    end_year = int(sys.argv[4])

    # Загрузить и обработать данные
    df, stats = load_and_process_data(
        base_path,
        start_year,
        end_year,
        output_path=output_path
    )
    print_statistics(stats)

    if df is not None:
        # Если output_path не был указан, предложить его
        if not output_path:
            default_output = Path(base_path).parent / f"dataset_{start_year}_{end_year or 'current'}.parquet"
            print(f"\nДля сохранения датасета используйте:")
            print(f"  python data_processor.py {base_path} {start_year} {end_year or datetime.now().year} {default_output}")

        print("\nПервые 5 строк датасета:")
        print(df.head())

        print("\nИнформация о датасете:")
        print(f"Форма: {df.shape}")
        print(f"Колонки: {list(df.columns)}")
        print(f"Типы данных:\n{df.dtypes}")
