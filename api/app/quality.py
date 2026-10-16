"""Row-level data quality indicators, also propagated through aggregates."""

# Screening, not a finding of incorrect classification or a replacement value.
SUSPECT_MASS = "(STRANA='TR' AND TYPE='fact' AND TNVED4='8703' AND EDIZM_ISO='796' AND KOL>0 AND NETTO>0 AND NETTO/KOL<100)"
QUALITY_SQL = f"""concat_ws('; ',
    NULLIF(PHYSICAL_QUALITY, ''),
    CASE WHEN NETTO_ESTIMATED THEN 'Масса оценена источником Comtrade, не nowcast проекта' END,
    CASE WHEN KOL_ESTIMATED THEN 'Количество оценено источником Comtrade, не nowcast проекта' END,
    CASE WHEN KOL>0 AND EDIZM_ISO IS NULL THEN 'Единица количества сохранена как в источнике; стандартный код не установлен' END,
    CASE WHEN NETTO IS NULL THEN 'Масса неизвестна или неполна' END,
    CASE WHEN {SUSPECT_MASS} THEN 'Проверить источник: масса менее 100 кг на транспортное средство; значения не исправлялись' END,
    CASE WHEN STRANA='TR' AND TNVED LIKE '27999999%' THEN 'Скрытая товарная детализация источника; не определять вид топлива по этому коду' END)
"""
