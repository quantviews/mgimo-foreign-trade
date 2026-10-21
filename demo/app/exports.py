"""Bounded, authenticated XLSX exports; no model-generated download URLs."""
from contextvars import ContextVar
from decimal import Decimal
import uuid
import base64

from . import db
from .numeric_facts import numerical_facts
from .analysis import monthly_evidence

# Per-request state, isolated between concurrent users.
export_context = ContextVar("export_context", default=None)
MAX_EXPORT_ROWS = 1000


def export_file_facts(rows, filters):
    """Describe the exported rows, not the database date or requested row count."""
    from .answer_style import fact
    months=sorted({str(r['period'])[:7] for r in rows if r.get('period')})
    if not months:
        return fact('export_file','В файле нет помесячной разбивки.')
    start,end=months[0],months[-1]
    first=int(start[:4])*12+int(start[5:7])-1
    last=int(end[:4])*12+int(end[5:7])-1
    expected={f'{n//12:04d}-{n%12+1:02d}' for n in range(first,last+1)}
    text=f'**В Excel: {period_label(months)}; месяцев с данными: {len(months)}.**'
    if expected-set(months):
        text+=' Месяцы с данными: '+', '.join(month_label(m) for m in months)+'.'
    requested=coverage_evidence(rows,filters).get('requested_months',[])
    missing=sorted(set(requested)-set(months))
    if missing:
        text+=' В запрошенном периоде нет строк за: '+', '.join(month_label(m) for m in missing)+'.'
    predicted=[r for r in rows if r.get('type')=='pred' or r.get('source')=='nowcast']
    if predicted:
        observed={str(r['period'])[:7] for r in rows if r.get('period') and r.get('type')=='fact' and r.get('source')!='nowcast'}
        forecast={str(r['period'])[:7] for r in predicted if r.get('period')}
        text+=f' Статистика источников: {len(observed)} мес.; nowcast проекта: {len(forecast)} мес. ('+', '.join(month_label(m) for m in sorted(forecast))+').'
        absent=sorted(set(requested)-observed)
        if absent:
            text+=' Месяцы без строк статистики источников: '+', '.join(month_label(m) for m in absent)+'.'
    return fact('export_file',text)


def export_monthly_facts(rows):
    """Present the exact workbook rows, preserving units, nulls and source status."""
    from .answer_style import fact
    from .numeric_facts import fmt, display_scale, number
    groups={}
    for row in rows:
        if not row.get('period'):
            return {}
        key=(row.get('country_name') or COUNTRIES.get(row.get('strana'),row.get('strana','')),row.get('type'),row.get('source'),row.get('comparison_period'))
        groups.setdefault(key,[]).append(row)
    result={}
    if len(rows)>60:
        return fact('export_monthly','Полный помесячный ряд находится в Excel. В файле больше 60 строк; таблица в чате не показана.')
    for (country,kind,source,comparison_period),part in groups.items():
        predicted=kind=='pred' or source=='nowcast'
        label='Nowcast проекта' if predicted else {'national':'Национальная статистика','comtrade':'UN Comtrade'}.get(source,source or 'Источник не указан')
        title=f'**{country}. {label}**'
        if comparison_period:
            title+=' ('+{'current':'текущий период','previous':'предыдущий период'}.get(comparison_period,comparison_period)+')'
        scales={m:display_scale(m,[r.get(m) for r in part]) for m in ('stoim','netto')}
        lines=[title,'',f'| Месяц | Стоимость, {scales["stoim"][1]} | Количество | Единица | Масса, {scales["netto"][1]} |','|---|---:|---:|---|---:|']
        for row in sorted(part,key=lambda r:(str(r['period']),str(r.get('edizm') or ''))):
            def value(metric):
                v=number(row.get(metric))
                return fmt(v/scales[metric][0]) if v is not None else 'нет данных'
            unit=str(row.get('edizm') or 'не указана').replace('|','/').replace('\n',' ')
            if unit=='?':unit='не указана'
            if unit.upper()=='ШТУКА':unit='шт.'
            quantity=quantity_number(row['kol']) if row.get('kol') is not None else 'нет данных'
            lines.append(f'| {month_label(row["period"])} | {value("stoim")} | {quantity} | {unit} | {value("netto")} |')
        if any(r.get('netto') is None or r.get('kol') is None for r in part):
            lines+=['','Пустые значения в Excel обозначены здесь как «нет данных».']
        if any(r.get('stoim')==0 for r in part):
            lines+=['','Нули стоимости сохранены из исходных строк; они не заполняют отсутствующие месяцы.']
        result.update(fact('export_monthly','\n'.join(lines)))
    return result


def coverage_evidence(rows, filters):
    def mid(v):
        y,m=map(int,str(v)[:7].split('-'));return y*12+m-1
    try:
        start,end=mid(filters['period_from']),mid(filters['period_to'])
        expected=[f'{v//12:04d}-{v%12+1:02d}' for v in range(start,end+1)]
    except (KeyError,ValueError,TypeError): return {}
    fact=[r for r in rows if r.get('type')=='fact' and r.get('source')!='nowcast' and str(r.get('period',''))[:7] in expected]
    observed=sorted({str(r['period'])[:7] for r in fact})
    return {'requested_months':expected,'observed_fact_months':observed,
        'missing_fact_months':sorted(set(expected)-set(observed)),
        'zero_value_rows':sum(r.get('stoim')==0 for r in fact),
        'missing_mass_rows':sum(r.get('netto') is None for r in fact),
        'missing_quantity_rows':sum(r.get('kol') is None for r in fact),
        'instruction':'Назови запрошенный период и отдельно месяцы имеющихся записей. При missing_fact_months сумма относится только к доступным записям, полный итог запрошенного периода не установлен. Не называй поставки малыми/редкими по неполному срезу. При пропусках физических показателей количество и масса только известная часть. Нулевая стоимость в существующей строке не равна отсутствию строки: пиши «в исходных записях указано 0», не «наблюдений нет». Не утверждай причину нуля (округление, отсутствие торговли, непубликация) без подтверждения источника. Отсутствующий месяц не заменяй нулем. Доли и вклады только при explanation.available=true за указанные сопоставимые периоды.'}


async def save_export(rows, meta, filters, prepared):
    ctx = export_context.get()
    if not ctx or ctx.get("user_id") is None:
        raise ValueError("Authenticated user required for export")
    if meta.get("has_more") or not rows or len(rows) > MAX_EXPORT_ROWS:
        raise ValueError("Export is incomplete or empty")
    content = base64.b64decode(prepared["xlsx_base64"], validate=True)
    normalized = rows
    totals = {(r["strana"], r["type"]): [Decimal(r["stoim"] or "0"), Decimal(r["netto"] or "0")] for r in prepared["summary"]}
    columns = {k for row in normalized for k in row}
    if len(content) > 5_000_000:
        raise ValueError("Export exceeds size limit")
    export_id = uuid.uuid4()
    pool = await db._require_pool()
    await pool.execute("DELETE FROM demo_exports WHERE expires_at < now()")
    await pool.execute(
        "INSERT INTO demo_exports(id,user_id,xlsx_content) VALUES($1,$2,$3)",
        export_id, ctx["user_id"], content)
    comparison_result = prepared.get('comparison', {})
    if prepared.get('file_manifest'):
        manifest = prepared['file_manifest']
        periods = manifest['periods']
        answer = (f"Excel содержит исходные строки за текущий период {periods['current'][0]} - {periods['current'][1]} "
                  f"и те же месяцы годом ранее: {periods['previous'][0]} - {periods['previous'][1]}. "
                  "Лист «Факт» содержит статистику источников, «Nowcast» - оценки проекта отдельно, "
                  "«Описание» - единицы, фильтры и методологию. Расчетных долей и вкладов в файле нет.")
    elif comparison_result.get('comparable'):
        answer = comparison_result['answer_ru']
        physical = comparison_result.get('physical_volume', {})
        if physical.get('reason') and physical['reason'] not in answer:
            answer += '\n\nИндекс физического объема: ' + physical['reason']
        answer += (f'\n\n[Скачать сравнение и помесячные данные в Excel](/demo/exports/{export_id})'
                   '\n\nЛист «Сравнение» содержит те же показатели и общие месяцы факта, что и ответ. '
                   'Лист «Данные» сохраняет весь выбранный помесячный ряд, включая оценки, если они запрошены. '
                   'Источники, фильтры и методология fizob приведены на листе «Описание». Ссылка действует 7 дней.')
    else:
        answer = render_summary(normalized, totals, columns, filters, meta, export_id)
    flags = sorted({r.get('quality_flags') for r in rows if r.get('quality_flags')})
    if flags:
        answer += '\n\n**Качество данных:** ' + '; '.join(flags) + '. Неизвестная масса не считается нулем.'
        from .answer_style import fact
        ctx['export_caveats'] = fact('export_caveat', '**Качество данных:** ' + '; '.join(flags) + '.')
    if ctx.get("region_note"):
        answer += "\n\n" + ctx["region_note"]
    ctx["answer"] = answer
    ctx["download_link"] = f"[Скачать данные в Excel](/demo/exports/{export_id})"
    ctx['export_file_facts'] = export_file_facts(rows, filters)
    ctx['export_monthly_facts'] = export_monthly_facts(rows)
    ctx["analysis_evidence"] = {'verified_summary':answer,'meta':meta,'filters':filters,
                               'data':rows[:40], 'preview_only':len(rows)>40,
                               'numeric_facts':numerical_facts(rows,filters),
                               'coverage_evidence':coverage_evidence(rows,filters),
                               **(monthly_evidence(rows) if len(rows)<=40 else {})}
    return {"export_ready": True, "rows": len(rows), "period_max": meta.get("period_max"), "file_manifest": prepared.get("file_manifest"),
            "coverage": {period: {"rows": len(part),
                "months": sorted({str(r.get("period", ""))[:7] for r in part}),
                "missing_quantity_rows": sum(r.get("kol") is None for r in part),
                "missing_mass_rows": sum(r.get("netto") is None for r in part),
                "sources": sorted({str(r.get("source", "")) for r in part})}
                for period in ("current", "previous")
                for part in [[r for r in rows if r.get("comparison_period")==period and r.get("type")=="fact" and r.get("source")!="nowcast"]]},
            "coverage_note": "Наличие 12 месяцев не гарантирует полноту товарного покрытия. Если missing_quantity_rows или missing_mass_rows больше нуля, полный итог этого показателя не рассчитан; известную часть нельзя называть общим количеством или массой."}


COUNTRIES = {"CN": "Китай", "IN": "Индия", "TR": "Турция", "DE": "Германия",
             "US": "США", "JP": "Япония", "KR": "Республика Корея", "BR": "Бразилия",
             "KZ": "Казахстан", "BY": "Беларусь", "EG": "Египет", "AE": "ОАЭ",
             "UZ": "Узбекистан", "NL": "Нидерланды", "HU": "Венгрия", "SK": "Словакия"}
MONTHS = ["", "янв.", "фев.", "мар.", "апр.", "май", "июн.", "июл.", "авг.", "сен.", "окт.", "ноя.", "дек."]


def ru_number(value, places=1):
    return f"{value:,.{places}f}".replace(",", "\u00a0").replace(".", ",")


def quantity_number(value):
    """Keep fractional source quantities; integer counts still use grouping."""
    value = Decimal(str(value))
    places = max(0, -value.normalize().as_tuple().exponent)
    return ru_number(value, places)


def month_label(value):
    value = str(value)
    try:
        return f"{MONTHS[int(value[5:7])]} {int(value[:4])}"
    except (ValueError, IndexError):
        return value


def period_label(periods):
    ordered = sorted(set(str(p)[:7] for p in periods if p))
    if not ordered: return "не указан"
    if len(ordered) == 1: return month_label(ordered[0])
    return month_label(ordered[0]) + " - " + month_label(ordered[-1])


def comparison(rows, columns, filters, names):
    countries = sorted(set(filters.get("strana") or [r.get("strana") for r in rows if r.get("strana")]))
    if len(countries) < 2: return []
    metrics = [m for m in ("stoim", "netto") if m in columns]
    if not metrics: return []
    buckets = {}
    for r in rows:
        if r.get('type') == 'fact' and r.get('period'):
            buckets.setdefault((r.get('strana'), str(r['period'])[:7]), []).append(r)
    available = {c: {p for (country,p), group in buckets.items() if country==c
                     and all(r.get(m) is not None for r in group for m in metrics)} for c in countries}
    common = set.intersection(*(available[c] for c in countries))
    if not common:
        return ["**Сопоставимого периода фактических данных для всех выбранных стран нет.** Сравнение итогов не рассчитано.", ""]
    sums = {c: {m: sum((Decimal(str(r[m])) for r in rows if r.get("strana")==c and r.get("type")=="fact" and str(r.get("period"))[:7] in common and r.get(m) is not None),Decimal(0)) for m in metrics} for c in countries}
    ordered = sorted(common)
    first,last = ordered[0],ordered[-1]
    continuous = (int(last[:4])-int(first[:4]))*12+int(last[5:7])-int(first[5:7])+1 == len(common)
    label = period_label(common) if continuous else ", ".join(month_label(m) for m in ordered)
    lines = [f"**Сравнение по факту за одинаковые месяцы: {label} ({len(common)} мес.).**", "",
             "Месяцы без сопоставимых фактических наблюдений исключены. Это сравнение внутри запрошенного интервала, не перенос окна на более ранние месяцы.", "",
             "| Страна | Стоимость, млрд долл. | Масса, тыс. т |", "|---|---:|---:|"]
    for c in countries:
        v = ru_number(sums[c]['stoim']/Decimal(10**9),2) if 'stoim' in metrics else 'не запрошена'
        w = ru_number(sums[c]['netto']/Decimal(10**6),1) if 'netto' in metrics else 'не запрошена'
        lines.append(f"| {names.get(c,c)} | {v} | {w} |")
    if len(countries)==2:
        a,b = countries
        for m, caption, scale, unit in [('stoim','стоимости',Decimal(10**9),'млрд долл.'),('netto','массе',Decimal(10**6),'тыс. т')]:
            if m not in metrics: continue
            x,y = sums[a][m],sums[b][m]
            if x==y:
                lines += ['',f"По {caption} показатели {names.get(a,a)} и {names.get(b,b)} равны."]
                continue
            high,low = (a,b) if x>y else (b,a)
            hv,lv = sums[high][m],sums[low][m]
            pct = f" ({ru_number((hv-lv)/lv*100,1)}%; база сравнения: {names.get(low,low)})" if lv>0 else ''
            lines += ['',f"По {caption} **{names.get(high,high)} выше, чем {names.get(low,low)}, на {ru_number((hv-lv)/scale,2 if m == "stoim" else 1)} {unit}{pct}**."]
    lines += ['', 'Модельные оценки в это сравнение не входят. Совпадение месяцев не гарантирует одинаковую полноту товарного покрытия источников.', '', '#### Все данные запрошенного интервала', '']
    return lines


def render_summary(rows, totals, columns, filters, meta, export_id):
    names = dict(COUNTRIES)
    names.update({r["strana"]: r["country_name"] for r in rows if r.get("country_name") and r.get("strana") and r["strana"] not in names})
    direction = filters.get("napr", [])
    title = "Экспорт России" if direction in (["ex"], ["ЭК"]) else "Импорт России" if direction in (["im"], ["ИМ"]) else "Торговля России"
    codes = filters.get("tnved4", [])
    product = {"2709": "сырая нефть", "2710": "нефтепродукты", "8703": "легковые автомобили"}.get(codes[0]) if len(codes) == 1 else None
    if product: title += ": " + product
    periods = [r.get("period") for r in rows]
    kinds = {k[1] for k in totals}
    lines = ["### " + title, "", "**" + period_label(periods) + "**", ""]
    fact = sum((v[0] for k,v in totals.items() if k[1] == "fact"), Decimal(0))
    pred = sum((v[0] for k,v in totals.items() if k[1] == "pred"), Decimal(0))
    compared = comparison(rows, columns, filters, names)
    lines += compared
    if "stoim" in columns and not compared:
        if "fact" in kinds:
            lines.append(f"По выбранным странам и товарам стоимость поставок с фактическими данными составляет **{ru_number(fact / Decimal(10**9), 2)} млрд долл.**")
        if "pred" in kinds:
            pred_text = (ru_number(pred / Decimal(10**6), 2) + ' млн долл.') if abs(pred) < 10**7 else (ru_number(pred / Decimal(10**9), 2) + ' млрд долл.')
            lines.append(f"Отдельно показана **модельная оценка: {pred_text}**. Она не включена в итог по фактическим данным.")
    elif "stoim" not in columns:
        lines.append("Ниже объёмы поставок по выбранным странам и товарам. Стоимость не запрашивалась.")
    fact_counts = [r for r in rows if r.get('type') == 'fact']
    if codes == ['8703'] and fact_counts and all(str(r.get('edizm_iso')) == '796' and r.get('kol') is not None for r in fact_counts):
        count = sum((Decimal(str(r['kol'])) for r in fact_counts), Decimal(0))
        lines += ['', f"**Количество по факту: {quantity_number(count)} шт.** Модельные оценки в это число не включены."]
    lines += ["", "| Страна | Данные | Период | Стоимость, млрд долл. | Масса, тыс. т |", "|---|---|---|---:|---:|"]
    for (country, kind), (value, weight) in sorted(totals.items())[:20]:
        v = ru_number(value / Decimal(10**9), 2) if "stoim" in columns else "не запрошена"
        w = ru_number(weight / Decimal(10**6), 1) if "netto" in columns else "не запрошена"
        group = [r for r in rows if r.get('strana','ALL') == country and r.get('type','unspecified') == kind]
        if 'netto' in columns and any(r.get('netto') is None for r in group):
            w = 'нет полных данных'
        if 0 < abs(value) < 5_000_000 and 'stoim' in columns:
            v = ru_number(value / Decimal(10**9), 4) if abs(value) >= 50_000 else ('<0,0001' if value > 0 else '>-0,0001')
        span = period_label([r.get("period") for r in rows if r.get("strana", "ALL")==country and r.get("type", "unspecified")==kind and (kind != "pred" or any(r.get(m) is not None and Decimal(str(r[m])) != 0 for m in ("stoim", "netto", "kol") if m in columns))])
        name = str(names.get(country, country)).replace("|", " ").replace("\n", " ")
        kind_name = {"fact":"Факт", "pred":"Оценка"}.get(kind, "Не указан")
        lines.append(f"| {name} | {kind_name} | {span} | {v} | {w} |")
    if len(totals)>20: lines += ["", "В таблице первые 20 групп. Все группы доступны в Excel."]
    if 'kol' in columns:
        quantities = {}
        for row in rows:
            key = (row.get('strana'), row.get('type'), row.get('edizm'), row.get('edizm_iso'))
            quantities.setdefault(key, []).append(row.get('kol'))
        lines += ['', '#### Количество в дополнительных единицах', '', '| Страна | Данные | Единица | Количество |', '|---|---|---|---:|']
        for (country, kind, unit, code), values in sorted(quantities.items(), key=lambda kv: str(kv[0])):
            quantity = quantity_number(sum((Decimal(str(v)) for v in values), Decimal(0))) if unit and all(v is not None for v in values) else 'нет полных данных'
            estimated = any('Количество оценено источником Comtrade' in (r.get('quality_flags') or '')
                            for r in rows if (r.get('strana'),r.get('type'),r.get('edizm'),r.get('edizm_iso')) == (country,kind,unit,code))
            kind_label = 'Факт' if kind == 'fact' else 'Модельная оценка'
            if estimated: kind_label += '; количество включает оценку Comtrade'
            incomplete = any(r.get('kol') is None or not r.get('edizm') or r.get('edizm') in ('?', '-', 'N/A')
                             for r in rows if r.get('strana') == country and r.get('type') == kind)
            if incomplete and quantity != 'нет полных данных':
                quantity += ' (только известная часть; не полный итог)'
            lines.append(f"| {names.get(country,country)} | {kind_label} | {unit or 'не указана'} | {quantity} |")
    lines += ["", "Периоды фактических данных и оценок могут различаться. Пропуски не означают нулевые поставки.",
              "", f"[Скачать данные в Excel](/demo/exports/{export_id})", "",
              f"{ru_number(len(rows),0)} строк по месяцам. Источники, единицы и фильтры приведены на листе «Описание». Ссылка действует 7 дней.",
              "", "Актуальность базы: " + month_label(meta.get("period_max", "")) + "."]
    if any(r.get("type")=="pred" and all(r.get(m) is None or Decimal(str(r[m]))==0 for m in ("stoim","netto","kol") if m in columns) for r in rows):
        lines += ["", "В файле есть нулевые модельные строки. Они не меняют суммы и не расширяют показанный период оценки."]
    missing = set(filters.get("strana", [])) - {r.get("strana") for r in rows}
    if missing: lines += ["", "Нет наблюдений: " + ", ".join(names.get(c,c) for c in sorted(missing)) + "."]
    return "\n".join(lines)
