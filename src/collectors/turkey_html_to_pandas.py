"""
Парсер HTML таблиц с данными по Турции в Pandas DataFrame и сохранение в виде Parquet
"""

from bs4 import BeautifulSoup
from io import StringIO
import pandas as pd

# filename = "./turkey_data/turkey_html_data_2025/22042128-22042197-2025.html"  # null
# filename = "./turkey_data/turkey_html_data_2025/16023119-16041441-2025.html"  # small
filename = "./turkey_data/turkey_html_data_2025/84806000-84818079-2025.html"  # big
# filename = "./turkey_data/turkey_html_data_2025/84818081-84834021-2025.html"  # almost big
# filename = "./turkey_data/turkey_html_data_2025/84669340-84679900-2025.html"  # edge case

YEAR = "2025"


def table_clean(df, row_slice, col_slice):
    df.drop_duplicates(inplace=True)
    df = df.iloc[
        row_slice, col_slice
    ].copy()  # обрезать таблицу по определенным границам
    df.dropna(
        axis=1, how="all", inplace=True
    )  # удалить столбцы, все значения которых NaN
    df.drop(
        df[df.iloc[:, 3].str.contains(r"^\s*total\s*:?$", case=False, na=False)].index,
        inplace=True,
    )  # удалить строки с 'total' в 4-м столбце
    df.iloc[:, 0:3] = df.iloc[:, 0:3].ffill()  # заполнить NaN предыдущими значениями
    df.columns = df.iloc[0]  # первая строка — новые имена столбцов
    df = df[1:].reset_index(drop=True)  # отбросить первую строку и сбросить индекс
    df.columns.name = YEAR  # задать имя колонкам как год
    df = df.loc[:, ~df.columns.duplicated()]  # убрать дублирующиеся столбцы
    return df


def main():
    with open(filename, "r", encoding="utf-8", errors="ignore") as file:
        html_content = file.read()

    soup = BeautifulSoup(html_content, "html.parser").find_all("table")

    if len(soup) == 1:
        html_io = StringIO(str(soup[0]))
        df = pd.read_html(html_io, flavor="lxml")[0]
        if df.shape[0] > 36:
            df = table_clean(df, slice(4, -2), slice(3, 30))
        else:
            df = pd.DataFrame()
    else:
        dfs = []
        for table in soup:
            html_io = StringIO(str(table))
            df = pd.read_html(html_io, flavor="lxml")[0]
            dfs.append(df)

        if dfs[1].shape[0] == 26:
            dfs[0] = table_clean(dfs[0], slice(4, -2), slice(2, 30))
            df = dfs[0]
        else:
            dfs[0] = table_clean(dfs[0], slice(4, None), slice(2, 30))
            dfs[1] = table_clean(dfs[1], slice(1, -2), slice(2, 30))
            df = pd.concat(dfs, axis=0, ignore_index=True)
    df.to_parquet(filename + ".parquet")


if __name__ == "__main__":
    main()
