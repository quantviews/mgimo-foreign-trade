"""
Модуль выполняет следующие операции с исходными данными:
 - загружает данные в исходном формате из parquet файлов,
 - удаляет лишние столбцы,
 - добавляет отсутствующие столбцы, согласно гармонизированной модели данных,
 - добавляет информацию о единицах измерения и их ISO кода
 - сохраняет результирующую таблицу за все года в гармонизированном формате в виде parquet файла
"""


import pandas as pd

def compose_df(df: pd.DataFrame) -> pd.DataFrame:
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
    df["PERIOD"] = pd.to_datetime(df["PERIOD"].str.zfill(2).radd(f"{YEAR}-").add("-01"))

    # Страна и строковые столбцы
    df["STRANA"] = "TR"
    df["EDIZM_ISO"] = df["EDIZM"]

    # TNVED разбивка без многократного обращения к колонке
    df["TNVED4"] = df["TNVED"].str[:4]
    df["TNVED6"] = df["TNVED"].str[:6]
    df["TNVED2"] = df["TNVED"].str[:2]

    # Маппинг EDIZM и EDIZM_ISO
    edizm_map = {k: v[2] if len(v) > 2 else None for k, v in units.items()}
    edizm_iso_map = {k: v[0] if len(v) > 0 else None for k, v in units.items()}
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
        result[col] = pd.to_numeric(result[col], errors="coerce").fillna(0).astype(float)

    return result


if __name__ == "__main__":

    units = {
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

    PATH = "./data/turkey_parquet/"

    dfs = []

    for YEAR in range(2005, 2026):
        fname = PATH + f"turkey_{YEAR}.parquet"
        df = pd.read_parquet(fname)

        cols = df.columns[6:]
        for col in cols:
            df[col] = df[col].astype(str)

        dfs.append(compose_df(df))

    final_df = pd.concat(dfs, axis=0, ignore_index=True)
    final_df.to_parquet("turkey_normalized_full.parquet")
