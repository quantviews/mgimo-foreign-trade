"""
Модуль находит сырые данные в виде html файлов, производит их предварительную обработку и  объединяет в единый датасет,
который затем гармонизируется в соответствии с моделью данных и сохраняется в виде parquet файла.
"""

import re, os, argparse
from bs4 import BeautifulSoup
from pathlib import Path
from io import StringIO
import pandas as pd
import numpy as np
from datetime import datetime


def parse_arguments():
    """
    Обработчик аргументов для запуска модуля из командной строки.

    usage: processor.py [-h] [-a] year

    :return: возвращает список аргументов для запуска модуля
    """
    current_year = datetime.now().year
    parser = argparse.ArgumentParser(
        description="Module processes raw data, compiles normalized dataset and saves result into a parquet file"
    )

    def valid_year(value):
        if not value.isdigit():
            raise argparse.ArgumentTypeError(
                f"Year should be a number in range 2005-{current_year}"
            )
        year = int(value)
        if year < 2005 or year > current_year:
            raise argparse.ArgumentTypeError(
                f"Year should be a number in range 2005-{current_year}"
            )
        return value

    # Позиционные аргументы (обязательные)

    parser.add_argument(
        "year",
        type=valid_year,
        nargs="?",
        metavar=f"[2005-{current_year}]",
        help=f"year (from 2005 to {current_year})",
    )

    parser.add_argument(
        "-a",
        "--all",
        action="store_true",
        help="process data for the all available years",
    )

    # parser.add_argument("-v", "--verbose", action="store_true", help="verbose output")

    # Парсинг аргументов
    args = parser.parse_args()
    return args


def table_clean(df: pd.DataFrame, year) -> pd.DataFrame:
    """
    Функция производит очистку таблицы полученной из сырых данных.
    - удаляются дублирующиеся строки,
    - определяются позиции референсных ячеек для сепарации полезных данных от артефактов форматирования
    - заполняются отсутствующие значения в столбцах "Month", "Country", "Country name"
    - исправляются ошибки в кодах с ведущим нулем

    :param df: pandas.DataFrame
    :param year:
    :return: pd.DataFrame
    """
    # Удаление дубликатов
    df.drop_duplicates(inplace=True)

    # Поиск положения "Month" в таблице
    month_pos = np.where(df.values == "Month")
    if len(month_pos[0]) > 1:
        print("Can't parse: multiple 'Month' found")
        return pd.DataFrame()
    if len(month_pos[0]) == 0:
        print("One of the tables is empty: no 'Month' found")
        return pd.DataFrame()

    month_row, month_col = month_pos[0][0], month_pos[1][0]
    df = df.iloc[month_row:, month_col:].copy()

    # Если последняя строка в 4-м столбце начинается с "Note:" или содержит "provisional", удалить её.
    # Проверка на слово "provisional" выполняется отдельно, так как на странице могут быть оба совпадения.
    last_row_val = df.iloc[-1, 3]
    if isinstance(last_row_val, str) and last_row_val.startswith("Note:"):
        df = df.iloc[:-1].copy()
    last_row_val = df.iloc[-1, 3]
    if isinstance(last_row_val, str) and "provisional" in last_row_val:
        df = df.iloc[:-1].copy()

    # Удалить столбцы, полностью состоящие из NaN
    df.dropna(axis=1, how="all", inplace=True)

    # Маска для строк, содержащих цифры в 4-м столбце
    mask = df.iloc[:, 3].astype(str).str.contains(r"\d", na=False)
    if not mask.any():
        print("Table is empty: no digits in 4th column")
        return pd.DataFrame()

    last_idx = mask[mask].index[-1]
    df = df.loc[:last_idx].copy()

    # Удаление строк с "total" в 4-м столбце (регистронезависимо)
    total_mask = (
        df.iloc[:, 3]
        .astype(str)
        .str.fullmatch(r"\s*total\s*:?\s*", case=False, na=False)
    )
    df = df.loc[~total_mask].copy()

    # Первая строка — имена столбцов
    df.columns = df.iloc[0]
    df = df.iloc[1:]
    df.columns.name = year

    # Удаление дублированных названий столбцов
    df = df.loc[:, ~df.columns.duplicated()]

    # Заполнение пропусков в первых трёх столбцах вперёд
    df.iloc[:, 0:3] = df.iloc[:, 0:3].ffill()

    # Проверка и исправление ошибки в исходных данных с отсутствующим нулем в начале HS8 кода
    mask = df["HS8"].str.len() < 8

    df.loc[mask, "HS8"] = df.loc[mask, "HS8"].apply(
        lambda x: x if x.startswith("0") else "0" + x
    )

    # Удаление дублированных строк и сброс индекса
    df.drop_duplicates(inplace=True)
    df.reset_index(drop=True, inplace=True)

    # print(df[df["HS8"].str.len() < 8]["HS8"]) # найти и напечатать слишком короткие коды

    return df


def load_df(filename, year) -> pd.DataFrame:
    """
    Функция загружает данные из определенного html файла, обнаруживает в нем таблицы для обработки,
    производит их очистку с помощью функции table_clean() объединяет и возвращает pd.DataFrame с результатом.

    :param filename:
    :param year:
    :return: pd.DataFrame
    """
    with open(filename, "r", encoding="utf-8", errors="ignore") as file:
        html_content = file.read()

    soup = BeautifulSoup(html_content, "html.parser").find_all("table")

    dfs = [
        table_clean(pd.read_html(StringIO(str(table)), flavor="lxml")[0], year)
        for table in soup
    ]

    # Фильтруем пустые DataFrame, чтобы избежать ошибок конкатенации
    dfs = [df for df in dfs if not df.empty]

    if dfs:
        return pd.concat(dfs, axis=0, ignore_index=True)
    else:
        print(f"No valid tables in file {os.path.basename(filename)}")
        return pd.DataFrame()


def harmonize_df(df: pd.DataFrame, year: str) -> pd.DataFrame:
    """
    Функция выполняет гармонизацию данных в соответствии с моделью данных.

    :param df:
    :param year:
    :return:
    """
    print("Harmonizing consolidated data...")
    # Переименование колонок и удаление лишних сразу
    df.rename(
        columns={
            "Month": "PERIOD",
            "Country": "STRANA",
            "HS8": "TNVED",
            "Unit": "EDIZM",
        },
        inplace=True,
    )
    df.drop(["Country\xa0name", "HS8\xa0name"], axis=1, inplace=True)

    # Форматирование периода
    df["PERIOD"] = pd.to_datetime(df["PERIOD"].str.zfill(2).radd(f"{year}-").add("-01"))

    # Страна и строковые столбцы
    df["STRANA"] = "TR"
    df["EDIZM_ISO"] = df["EDIZM"]

    # TNVED разбивка без многократного обращения к колонке
    df["TNVED4"] = df["TNVED"].str[:4]
    df["TNVED6"] = df["TNVED"].str[:6]
    df["TNVED2"] = df["TNVED"].str[:2]

    # Маппинг EDIZM и EDIZM_ISO
    edizm_map = {k: v[2] if len(v) > 2 else None for k, v in UNITS.items()}
    edizm_iso_map = {k: v[0] if len(v) > 0 else None for k, v in UNITS.items()}
    df["EDIZM"] = df["EDIZM"].map(edizm_map)
    df["EDIZM_ISO"] = df["EDIZM_ISO"].map(edizm_iso_map)

    # Маски выборки: вместо index/loc — boolean index
    mask_in = df["Export\xa0Dollar"] != "0"
    mask_out = df["Import\xa0Dollar"] != "0"

    # inbound (ИМ)
    df_in = df[mask_in].copy()
    df_in.drop(
        ["Import\xa0quantity\xa01", "Import\xa0quantity\xa02", "Import\xa0Dollar"],
        axis=1,
        inplace=True,
    )
    df_in.rename(
        columns={
            "Export\xa0quantity\xa01": "NETTO",
            "Export\xa0quantity\xa02": "KOL",
            "Export\xa0Dollar": "STOIM",
        },
        inplace=True,
    )
    df_in["NAPR"] = "ИМ"

    # outbound (ЭК)
    df_out = df[mask_out].copy()
    df_out.drop(
        ["Export\xa0quantity\xa01", "Export\xa0quantity\xa02", "Export\xa0Dollar"],
        axis=1,
        inplace=True,
    )
    df_out.rename(
        columns={
            "Import\xa0quantity\xa01": "NETTO",
            "Import\xa0quantity\xa02": "KOL",
            "Import\xa0Dollar": "STOIM",
        },
        inplace=True,
    )
    df_out["NAPR"] = "ЭК"

    # Итоговое объединение
    result = pd.concat([df_in, df_out], ignore_index=True)

    cols = [
        "NAPR",
        "PERIOD",
        "STRANA",
        "TNVED",
        "EDIZM",
        "EDIZM_ISO",
        "STOIM",
        "NETTO",
        "KOL",
        "TNVED4",
        "TNVED6",
        "TNVED2",
    ]
    result = result[cols]
    result.sort_values(by="PERIOD", inplace=True)
    result.reset_index(drop=True, inplace=True)

    # Преобразование типов
    for col in ["STOIM", "NETTO", "KOL"]:
        clean_col = (
            result[col]
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
        )
        result[col] = pd.to_numeric(clean_col, errors="coerce").fillna(0).astype(float)

    return result


def build_for_year(year):
    """
    Функция загружает обработанные и очищенные данные с помощью функции load_df() из всех html файлов за определенный
    год и возвращает результирующий датасет за определенный год.

    :param year:
    :return:
    """

    html_files_path = Path.cwd() / "raw_html_tables" / str(year)

    if not html_files_path.is_dir():
        print(
            f"{html_files_path} folder is absent. Can't fetch raw html tables for {year}."
        )
    else:
        pattern = re.compile(r"\d{2,10}-\d{2,10}-20\d{2}\.html")
        html_files = [
            f
            for f in html_files_path.iterdir()
            if f.is_file() and pattern.match(f.name)
        ]
        html_files.sort()
        if not html_files:
            print(f"There are no required files for {year}.")
        else:
            dfs = []
            for index, f in enumerate(html_files):
                print(f"{index + 1}/{len(html_files)} Working with: {f.name}")
                df = load_df(f, year)
                if not df.empty:
                    dfs.append(df)

        if dfs:
            final_df = pd.concat(dfs, axis=0, ignore_index=True)

            # Сбросить пустые строки
            final_df = final_df.dropna(subset=[final_df.columns[3]]).reset_index(
                drop=True
            )

            return final_df

        else:
            print(f"No data to process for {year}")


def main():
    args = parse_arguments()

    if args.all:

        pattern = re.compile(r"20\d{2}")

        # Находим все папки с данными по годам
        working_dir = Path.cwd() / "raw_html_tables"
        years = sorted(
            [
                p.name
                for p in working_dir.iterdir()
                if p.is_dir() and pattern.fullmatch(p.name)
            ]
        )

        if years:
            dfs = []
            for year in years:
                print(f"\n{'=' * 30}\n Processing {year}")
                df = build_for_year(year)
                df = harmonize_df(df, year)
                dfs.append(df)

            full_df = pd.concat(dfs, axis=0, ignore_index=True)
            full_df.to_parquet("tr_full.parquet")
            print(
                'Data consolidation and harmonization completed. \nFile "tr_full.parquet" was saved.'
            )

        else:
            print("No folders with data.")

    elif args.year:
        df = build_for_year(args.year)
        df = harmonize_df(df, args.year)

        df.to_parquet(f"turkey_{args.year}_processed.parquet")
        print(
            f'Data consolidation and harmonization completed. \nFile "turkey_{args.year}_processed.parquet" was saved.'
        )
    else:
        print(
            "Use a 'year' option to extract data for a specific year or '--all' for all available years"
        )


if __name__ == "__main__":
    # словарь который используется для конвертации единиц измерения при гармонизации данных
    UNITS = {
        "KG/ÇİFT": ["715", "ПАР", "ПАРА"],
        "KG": ["?", "?", "?"],
        "KG/METR E": ["006", "МЕТР", "М"],
        "KG/1000A DET": ["798", "ТЫСЯЧА ШТУК", "1000 ШТ"],
        "KG/KG P2O5": ["865", "КИЛОГРАММ ПЯТИОКИСИ ФОСФОРА", "КГ P2O5"],
        "KG/ADET": ["796", "ШТУКА", "ШТ"],
        "KG/M3": ["113", "КУБИЧЕСКИЙ МЕТР", "М3"],
        "KG/KG K2O": ["852", "КИЛОГРАММ ОКСИДА КАЛИЯ", "КГ K2O"],
        "KG/KG MET.AM.": ["?", "КИЛОГРАММ МЕТИЛАМИНА", "KG MET.AM"],
        "KG/1000LI TRE": ["130", "1000 ЛИТРОВ", "1000 Л"],
        "KG/CE-El": ["745", "ЭЛЕМЕНТ", "ЭЛЕМ"],
        "KG/LİTRE": ["112", "ЛИТР", "Л"],
        "KG/BAŞ": ["836", "ГОЛОВА", "ГОЛ"],
        "KG/KARA T": ["162", "МЕТРИЧЕСКИЙ КАРАТ(1КАРАТ=2*10(-4)КГ", "КАР"],
        "KG/100AD ET": ["797", "СТО ШТУК", "100 ШТ"],
        "KG/KG\xa0N": ["861", "КИЛОГРАММ АЗОТА", "КГ N"],
        "KG/M2": ["055", "КВАДРАТНЫЙ МЕТР", "М2"],
        "KG/LT- ALK%100": ["831", "ЛИТР ЧИСТОГО (100%) СПИРТА", "Л 100% СПИРТА"],
        "KG/KG H2O2": ["841", "КИЛОГРАММ ПЕРОКСИДА ВОДОРОДА", "КГ H2O2"],
        "KG/GRAM": ["163", "ГРАММ", "Г"],
        "KG/KG\xa0U": ["867", "КИЛОГРАММ УРАНА", "КГ U"],
        "KG/1000M 3": ["114", "1000 КУБИЧЕСКИХ МЕТРОВ", "1000 М3"],
        "KG/gi\xa0F/S": ["?", "?", "gi F/S"],
        "-": ["?", "?", "?"],
        "KG/CT-L": ["?", "?", "CT-L"],
        "G.T/ADET": ["796", "ШТУКА", "ШТ"],
        "KG/KG NET\xa0EDA": ["?", "?", "KG NET EDA"],
        "KG/KG %90\xa0SDT": ["845", "КИЛОГРАММ СУХОГО НА 90 % ВЕЩЕСТВА", "КГ 90% С/В"],
        "KG/KG KOH": ["859", "КИЛОГРАММ ГИДРОКСИДА КАЛИЯ", "КГ KOH"],
        "KG/KG NaOH": ["863", "КИЛОГРАММ ГИДРОКСИДА НАТРИЯ", "КГ NAOH"],
    }
    main()
