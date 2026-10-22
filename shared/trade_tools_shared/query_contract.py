"""Scope and completeness contract shared with remote MCP."""
RULES = """
Перед любым запросом торговли установи маршрут товара. База содержит только торговлю России с партнерами. Обязательно вызови resolve_trade_route(origin, destination, question) с дословным последним вопросом пользователя, если вопрос описывает импорт/экспорт с точки зрения другой страны или маршрут неясен. Используй возвращенные strana и napr во всех следующих инструментах, включая Excel и fizob.
Импорт Китаем/Индией ИЗ России = Россия -> CN/IN = ex (ЭК). Импорт России ИЗ Китая = CN -> Россия = im (ИМ). Экспорт Турции в Россию = im. Слово «импорт» само по себе не задает napr.
Импорт США без указания России: уточни, нужен ли импорт из России; не выдавай двусторонние данные за весь импорт США. Для «Индия из России и других стран» покажи российскую часть с ex, кратко сообщи границу охвата остальных поставщиков. Не отказывай в доступной части.
Пустой результат относится только к указанным фильтрам. Проверь маршрут, код и даты перед утверждением об отсутствии данных. Не меняй направление ради непустого результата и не заменяй 2709 всей главой 27 без согласования.
Полнота относится к конкретному вызову. Усечение передачи/пагинация не означает отсутствие статистики: уточни группировку, запроси страницы или разбей аналитическое чтение по странам. Не обходи квоты выгрузок. Полный повторный запрос заменяет неполный для того же среза. Используй доступные полные срезы; отсутствие других месяцев не запрещает показать имеющиеся. Не считай общий итог из усеченной выборки. Не создавай Excel без просьбы.
Не перечисляй предполагаемые ISO-коды всех стран: отсутствие фильтра strana означает все доступные страны. По умолчанию без дат показывай текущий доступный год, не начинай произвольно с 2021 года. Для динамики год к году обязательно запрашивай те же месяцы предыдущего года: годовой итог за весь прошлый год не является итогом за январь-август. Не вычисляй суммы в уме, запрашивай серверные агрегаты на точные окна. Не переноси даты/числа между строками. Используй дефисы вместо длинного тире.
Для нефти по ключевым партнерам сначала получи полные итоги по strana, выбери лидеров, затем помесячные ряды отдельно по ним. Сохраняй type/source. Стоимость/масса дает среднюю удельную стоимость, а не рыночную цену. Индекс fizob запрашивай отдельно с тем же направлением и периодом.
"""


def resolve_trade_route(origin: str, destination: str, question: str = "") -> dict:
    """Resolve ISO-2 origin/destination into Russian-perspective filters; UNKNOWN for unspecified side."""
    origin, destination = origin.strip().upper(), destination.strip().upper()
    direction = explicit_direction(question)
    if direction:
        partner = destination if origin == 'RU' else origin
        if partner == 'RU': partner = 'WORLD'
        origin, destination = ('RU', partner) if direction == 'ex' else (partner, 'RU')
    if origin == "RU" and destination not in ("RU", "UNKNOWN", "WORLD", ""):
        return {"supported": True, "origin": origin, "destination": destination, "strana": destination, "napr": "ex"}
    if destination == "RU" and origin not in ("RU", "UNKNOWN", "WORLD", ""):
        return {"supported": True, "origin": origin, "destination": destination, "strana": origin, "napr": "im"}
    if origin == "RU" and destination == "WORLD":
        return {"supported": True, "napr": "ex", "scope": "available Russian export partners; not guaranteed world total"}
    if destination == "RU" and origin == "WORLD":
        return {"supported": True, "napr": "im", "scope": "available Russian import partners; not guaranteed world total"}
    return {"supported": False, "needs_clarification": "UNKNOWN" in (origin, destination),
            "instruction": "Only Russia bilateral trade is covered. Clarify missing Russia connection; for mixed scope answer the Russian part separately."}


def safe_result(obj: dict) -> dict:
    """Never feed an incomplete row preview to the model as usable aggregate evidence."""
    if obj.get("_truncated") or obj.get("meta", {}).get("has_more"):
        return {"meta": obj.get("meta", {}), "result_status": "needs_refinement",
                "instruction": "Technical pagination/preview limit, not missing source data. Increase limit up to 1000 for country aggregates, do NOT decrease it repeatedly. This call supplies no usable totals. Retry with server-side aggregation or bounded country/month slices; use offset/next_offset where supported. Other complete calls remain usable. Do not infer zero trade or require Excel.",
                "preview_rows_withheld": len(obj.get("data", []))}
    return obj


def explicit_direction(question: str) -> str | None:
    """Only unambiguous Russian flow wording; never guess from data availability."""
    import re
    q = question.lower()
    if re.search(r"импорт\w*\s+и\s+экспорт|экспорт\w*\s+и\s+импорт", q):
        return None
    outward = bool(re.search(r'из\s+росси[иӣ]\b|экспорт\w*\s+росси[ия]\b', q))
    inward = bool(re.search(r'в\s+россию\b|импорт\w*\s+(?:в\s+)?росси[ия]\b', q))
    if outward == inward:
        return None
    return 'ex' if outward else 'im'
