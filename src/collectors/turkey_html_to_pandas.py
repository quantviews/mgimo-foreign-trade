"""
Скрипт загружает страницу с html таблицами, очищает их, конвертирует в Pandas.DataFrame и сохраняет в виде parquet файла
"""

from bs4 import BeautifulSoup
from io import StringIO
import pandas as pd
import numpy as np
import os, glob


def table_clean(df, year):
    # Удаление дубликатов
    df = df.drop_duplicates().copy()

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
    df = df.iloc[1:].reset_index(drop=True)
    df.columns.name = year

    # Удаление дублированных названий столбцов
    df = df.loc[:, ~df.columns.duplicated()]

    # Заполнение пропусков в первых трёх столбцах вперёд
    df.iloc[:, 0:3] = df.iloc[:, 0:3].ffill()

    return df


def main(filename, year):
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


if __name__ == "__main__":

    YEAR = "2009"
    PATH = f"./turkey_data/turkey_html_data_{YEAR}/"
    html_files = glob.glob(os.path.join(PATH, "*.html"))

    dfs = []
    for index, f in enumerate(html_files):
        fname = f"file://{os.path.abspath(f)}"
        print(f"{index} Working with: \n{fname}")
        df = main(f, YEAR)
        if not df.empty:
            dfs.append(df)

    if dfs:
        final_df = pd.concat(dfs, axis=0, ignore_index=True)

        # Сбросить пустые строки
        final_df = final_df.dropna(subset=[final_df.columns[3]]).reset_index(drop=True)

        # Выгрузить в parquet файл
        final_df.to_parquet(f"turkey_{YEAR}.parquet")
        print(f"Saved parquet file: turkey_{YEAR}.parquet")
    else:
        print("No data to save.")
