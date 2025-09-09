"""
Шаблон для парсера из html в пандас
"""

from bs4 import BeautifulSoup
from io import StringIO
import pandas as pd

filename = "./turkey_data/19052010-20019020-2025.html"
YEAR = "2025"

with open(filename, "r", encoding="utf-8", errors="ignore") as file:
    html_content = file.read()

soup = BeautifulSoup(html_content, "html.parser")
html_io = StringIO(str(soup))
df = pd.read_html(html_io, flavor="lxml")[0]

df = df.iloc[:, 2:30]  # сразу убрать первые 2 и все, начиная с 31-го
df = df.dropna(
    subset=[df.columns[3]]
)  # Удалить строки, где в 4-м столбце (по индексу) есть NaN
df = df.drop_duplicates()  # Удалить дубликаты
df = df.iloc[1:-2]  # Удалить первую и последние две строки
if df.shape[0] > 0:
    df = df.dropna(axis=1, how="all")  # Удалить столбцы, где все значения NaN
    # Сделать первую строку заголовками и сбросить индекс
    df.columns = df.iloc[0]
    df = df[1:].reset_index(drop=True)
    df.columns.name = YEAR
else:
    print(f"no data for ")

print(df)
