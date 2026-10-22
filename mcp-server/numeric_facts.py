"""Deterministic numeric statements and reference-only answer validation."""
from collections import defaultdict
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import hashlib
import re

INSTRUCTION = '''Числовые выводы оформляй ссылками {{fact:ID}} из numeric_facts. Каждая ссылка заменяется готовым проверенным предложением или таблицей. Не переписывай цифры, даты, проценты, экстремумы и направления изменения своими словами. Вне ссылок допустимы краткие вводные фразы и заголовки без чисел и утверждений о росте/снижении. Выбирай итоги, динамику и экстремумы по вопросу, затем таблицу при запросе помесячного ряда. Показывай отдельно nowcast и пропуски. Для недоступного расчета используй соответствующий факт-оговорку. Не выводи все факты подряд. Не добавляй собственные ссылки скачивания: они передаются сервисом отдельно. Это формат черновика; перед публикацией вызови verify_trade_answer, используй его answer без изменений.'''
DIMS = ('strana','napr','type','source','edizm','edizm_iso','tnved','tnved2','tnved4','tnved6','tn_level','tn_code')
COUNTRIES={'CN':'Китай','IN':'Индия','TR':'Турция','SK':'Словакия','HU':'Венгрия','US':'США',
           'KZ':'Казахстан','UZ':'Узбекистан','AM':'Армения','AZ':'Азербайджан',
           'GE':'Грузия','DE':'Германия'}

def short_product_name(code, name):
    """Editorial labels for known codes; preserve official names in source exports."""
    labels={'6107':'Мужское трикотажное белье, одежда для сна и домашние халаты',
            '8703':'Легковые автомобили',
            '870310':'Снегоходные транспортные средства, гольф-кары и аналогичные машины',
            '870321':'Бензиновые автомобили, двигатель до 1 л включительно',
            '870390':'Прочие легковые автомобили',
            '870322':'Бензиновые автомобили, двигатель свыше 1 до 1,5 л',
            '870323':'Бензиновые автомобили, двигатель свыше 1,5 до 3 л',
            '870360':'Подзаряжаемые гибриды с бензиновым двигателем',
            '870340':'Гибриды с бензиновым двигателем без внешней зарядки',
            '870370':'Подзаряжаемые гибриды с дизельным двигателем',
            '870380':'Электромобили'}
    return labels.get(str(code), ' '.join((name or 'Название отсутствует').split()).capitalize()
                      if (name or '').isupper() else ' '.join((name or 'Название отсутствует').split()))

def number(v):
    try:
        d=Decimal(str(v))
        return d if d.is_finite() else None
    except (InvalidOperation,ValueError,TypeError):return None

def fmt(v, places=2):
    if v is None:return 'нет данных'
    d=number(v)
    rounded=d.quantize(Decimal(10)**-places,rounding=ROUND_HALF_UP)
    if d and not rounded:
        # Retain small nonzero values without leaking binary floating-point tails.
        return format(d.quantize(Decimal(10)**(d.adjusted()-3),rounding=ROUND_HALF_UP).normalize(),'f').replace('.',',')
    return format(rounded,',f').replace(',',' ').replace('.',',')

def mid(p):
    try:
        y,m=map(int,str(p)[:7].split('-'))
        return y*12+m-1 if 1<=m<=12 else None
    except (ValueError,TypeError):return None

def month(m):return f'{m//12:04d}-{m%12+1:02d}'

def display_scale(metric, values):
    magnitude=max((abs(number(v)) for v in values if number(v) is not None),default=Decimal(0))
    if metric=='stoim':
        return (Decimal(10)**9,'млрд долл.') if magnitude>=10**9 else (
            (Decimal(10)**6,'млн долл.') if magnitude>=10**6 else (
                (Decimal(1000),'тыс. долл.') if magnitude>=1000 else (Decimal(1),'долл.')))
    return (Decimal(10)**6,'тыс. т') if magnitude>=10**6 else (
        (Decimal(1000),'т') if magnitude>=1000 else (Decimal(1),'кг'))

def index_baseline_filters(filters, rows):
    """Same slice, twelve preceding calendar months, never inferred offsets."""
    months=[mid(r.get('period',r.get('PERIOD'))) for r in rows]
    months=[m for m in months if m is not None]
    start=mid(filters.get('period_from'))
    if start is None:
        if not months:return None
        start=min(months)
    return {**filters, 'period_from':month(start-12)+'-01',
            'period_to':month(start-1)+'-01', 'offset':0, 'limit':10000}

def index_facts(selected, lookup, label, start, end, add):
    values={m:number(r.get('idx')) for m,r in selected.items()}
    valid={m:v for m,v in values.items() if v is not None}
    if not valid:
        add('index_caveat',label+': значения индекса отсутствуют.')
        return
    first,last=min(valid),max(valid)
    base=valid[first]
    if base<=0:
        add('index_caveat',label+': начальное значение неположительное; индекс с базой начала периода и изменение за окно не рассчитаны.')
    else:
        change=(valid[last]/base-1)*100
        verb='вырос' if change>0 else 'снизился' if change<0 else 'не изменился'
        add('index_change',f'**{label}.**\n\nЗа {month(first)} - {month(last)} индекс физического объема {verb}'
            +(f' на **{fmt(abs(change),1)}%**' if change else '')+
            f'. При {month(first)} = 100 значение в {month(last)} составляет **{fmt(valid[last]/base*100,1)}**.')
    def yoy(m):
        previous=lookup.get(m-12,[])
        old=number(previous[0].get('idx')) if len(previous)==1 else None
        return (valid[m]/old-1)*100 if m in valid and old is not None and old>0 else None
    latest_yoy=yoy(last)
    if latest_yoy is not None:
        add('index_yoy',f'{label}: Изменение за {month(last)} к {month(last-12)} составляет **{fmt(latest_yoy,1)}% г/г**.')
    table=['| Месяц | Индекс ('+month(first)+' = 100) | Изменение г/г, % |','|---|---:|---:|']
    for m in range(start,end+1):
        value=valid.get(m)
        table.append('| '+month(m)+' | '+(fmt(value/base*100,1) if value is not None and base>0 else 'нет данных')+' | '+fmt(yoy(m),1)+' |')
    add('monthly_table',label+'\n\n'+'\n'.join(table))
    if len(valid)!=end-start+1:
        add('index_caveat',f'{label}: в окне {month(start)} - {month(end)} есть пропуски; сравнение концов относится к указанным доступным месяцам.')
    if any(yoy(m) is None for m in valid):
        add('index_caveat',label+': для части месяцев нет сопоставимой положительной базы прошлого года; соответствующие темпы г/г не рассчитаны.')
    add('index_methodology','Индекс сглажен за 12 месяцев. Начальный доступный месяц принят за 100 для удобства чтения. Изменение за выбранное окно и изменение год к году показаны отдельно.')

def numerical_facts(rows, filters=None, baseline=()):
    filters=filters or {};groups=defaultdict(list);facts={}
    def add(kind,text):
        fid=hashlib.sha256(text.encode()).hexdigest()[:16]
        facts[fid]={'kind':kind,'text':text.replace('долл..','долл.')}
    allrows=[{k.lower():v for k,v in r.items()} for r in rows]
    base=[{k.lower():v for k,v in r.items()} for r in baseline]
    # Restore single-valued filtered dimensions when API grouping omits them.
    for r in allrows+base:
        for dim in DIMS:
            value=filters.get(dim)
            if isinstance(value,list):value=value[0] if len(value)==1 else None
            if dim not in r and value is not None:r[dim]=value
    for r in allrows:groups[tuple(str(r.get(k) or '') for k in DIMS)].append(r)
    for key,items in groups.items():
        info=dict(zip(DIMS,key));country=COUNTRIES.get(info['strana'],info['strana'] or 'Выбранные партнеры')
        status='оценка проекта (nowcast)' if info['type']=='pred' or info['source']=='nowcast' else 'статистика источника' if info['type']=='fact' else 'тип данных не указан'
        source={'national':'национальная статистика','comtrade':'UN Comtrade','nowcast':'модель проекта'}.get(info['source'],'')
        code=next((info[k] for k in ('tnved','tnved6','tnved4','tnved2','tn_code') if info[k]),'')
        route={'ex':'экспорт России','ЭК':'экспорт России','im':'импорт России','ИМ':'импорт России'}.get(info['napr'],'выбранное направление')
        label=f'{country}, {route}'+(f', код {code}' if code else '')+f', {status}'+(f' ({source})' if source else '')
        is_index=any('idx' in r for r in items)
        if is_index:
            label=f'{country}, {route}, '+('все товары' if code in ('','0') else f'код {code}')
        indexed=defaultdict(list)
        for r in items:
            if mid(r.get('period')) is not None:indexed[mid(r['period'])].append(r)
        if not indexed:
            # Server aggregates are usable, but never invent their monthly coverage.
            for r in items:
                if not ('year' in r or len(items)==1): continue
                period = (f"; годовая группа {r['year']}" if r.get('year') is not None else '')
                period += '; фильтр '+str(filters.get('period_from','начало не указано'))+' - '+str(filters.get('period_to','конец не указан'))
                vals=[]
                for metric,title,scale,unit in [('stoim','стоимость',Decimal(10)**9,'млрд долл.'),('netto','масса',Decimal(10)**6,'тыс. т')]:
                    value=number(r.get(metric))
                    scale,unit=display_scale(metric,[value])
                    if value is not None:vals.append(title+' '+fmt(value/scale)+' '+unit)
                if vals:add('aggregate',label+period+': '+', '.join(vals)+'. Покрытие месяцев по этой группировке не установлено; для сопоставления периодов нужен месячный ряд.')
            continue
        start=mid(filters.get('period_from'));end=mid(filters.get('period_to'))
        start=min(indexed) if start is None else start;end=max(indexed) if end is None else end
        if end-start>119 or end<start:continue
        selected={m:rs for m,rs in indexed.items() if start<=m<=end}
        if not selected:continue
        label+=f'; окно {month(start)} - {month(end)}'
        missing=sorted(set(range(start,end+1))-set(selected))
        if not is_index:
            add('coverage',label+f': записи за {len(selected)} мес.'+(' Отсутствуют месяцы: '+', '.join(map(month,missing))+'. Полный итог окна не установлен.' if missing else ' Все месяцы окна представлены записями.')+'')
        if any(len(rs)!=1 for rs in selected.values()):
            add('limitation',label+': несколько строк на один месяц внутри выбранных измерений; для итогов нужна однозначная помесячная группировка.');continue
        selected={m:rs[0] for m,rs in selected.items()}
        metrics=[('stoim','Стоимость','млрд долл.',Decimal(10)**9),('netto','Масса','тыс. т',Decimal(10)**6)]
        metrics=[(k,t,unit,scale) for k,t,_,_ in metrics
                 for scale,unit in [display_scale(k,[r.get(k) for r in selected.values()])]]
        if info['edizm'] or info['edizm_iso']:
            metrics.append(('kol','Количество',info['edizm'] or info['edizm_iso'],Decimal(1)))
        if any('idx' in r for r in items):metrics=[('idx','Уровень индекса физобъема','ед. индекса',Decimal(1))]
        lookup=defaultdict(list)
        for r in base+items:
            if tuple(str(r.get(k) or '') for k in DIMS)==key and mid(r.get('period')) is not None:
                lookup[mid(r['period'])].append(r)
        if is_index:
            index_facts(selected,lookup,label,start,end,add)
            continue
        if 'kol' in {m[0] for m in metrics} or any('kol' in r for r in items):
            flags=any(r.get('quality_flags_raw') or r.get('quality_flags') for r in selected.values())
            paired=[(number(r.get('netto')),number(r.get('kol'))) for r in selected.values()]
            pieces=info['edizm_iso']=='796' or info['edizm'].strip().upper() in ('ШТУКА','ШТ','ШТ.','ШТУК')
            usable=paired and all(a is not None and a>=0 and b is not None and b>=0 for a,b in paired)
            if info['type']!='pred' and info['source']!='nowcast':
                if flags:
                    add('mean_mass',label+': средняя масса единицы не рассчитана: в исходных записях есть предупреждения о физических показателях. Флаг относится к исходным записям, а не обязательно к отношению агрегированных сумм. Это ограничение массы/количества, не вывод об ошибке стоимости.')
                elif pieces and usable and sum(b for a,b in paired)>0:
                    ratio=sum(a for a,b in paired)/sum(b for a,b in paired)
                    add('mean_mass',label+f': средняя масса по имеющимся парным значениям - **{fmt(ratio)} кг/шт.** (сумма массы / сумма количества). Это арифметическое отношение, не независимая проверка качества источника.'+
                        (' Полный период не представлен; результат относится только к имеющимся месяцам.' if missing else ''))
                else:
                    add('mean_mass',label+': средняя масса единицы не рассчитана: нужны масса и положительное количество в штуках для одного и того же набора наблюдений. Неизвестные значения не заменяются нулями.')
        for metric,title,unit,scale in metrics:
            values={m:number(r.get(metric)) for m,r in selected.items()}
            valid={m:v for m,v in values.items() if v is not None}
            if not valid:continue
            def amount(v):return fmt(v/scale,3 if metric=='idx' else 2)+' '+unit
            if metric!='idx':
                total=sum(valid.values(),Decimal(0));partial=bool(missing or len(valid)!=len(selected))
                add('total',label+f'. {title} '+('в доступных записях (неполный итог)' if partial else 'по имеющимся записям за окно')+': '+amount(total)+'.'+(f' Значение неизвестно в {len(selected)-len(valid)} месячных строках.' if len(valid)!=len(selected) else ''))
                for year in sorted({m//12 for m in selected}):
                    yy={m:v for m,v in valid.items() if m//12==year}
                    if not yy:continue
                    lo=max(start,year*12);hi=min(end,year*12+11)
                    add('year_total',label+f'. {title} за {month(lo)} - {month(hi)}: '+amount(sum(yy.values(),Decimal(0)))+(' по доступным значениям; итог неполный.' if len(yy)!=hi-lo+1 else ' по имеющимся записям.'))
            low=min(valid.values());high=max(valid.values())
            add('extrema',label+f'. {title}: минимум среди доступных месяцев '+amount(low)+' ('+', '.join(month(m) for m,v in sorted(valid.items()) if v==low)+'); максимум '+amount(high)+' ('+', '.join(month(m) for m,v in sorted(valid.items()) if v==high)+').')
            if info['type']=='fact' and metric!='idx':
                year=end//12;lo=max(start,year*12);current=list(range(lo,end+1));pairs=[]
                for m in current:
                    prev=lookup.get(m-12,[])
                    if m in valid and len(prev)==1 and number(prev[0].get(metric)) is not None:pairs.append((m,valid[m],number(prev[0][metric])))
                if len(pairs)==len(current) and pairs:
                    a=sum((p[1] for p in pairs),Decimal(0));b=sum((p[2] for p in pairs),Decimal(0));delta=a-b
                    pct=delta/b*100 if b>0 else None
                    add('yoy',label+f'. {title}, сопоставимые окна {month(lo)} - {month(end)} и {month(lo-12)} - {month(end-12)}: '+amount(a)+' против '+amount(b)+f'; изменение {amount(delta)}'+(f' ({fmt(pct)}% г/г).' if pct is not None else '; процент не рассчитан при неположительной базе.'))
                elif current:
                    add('limitation',label+f'. {title}: динамика итогов год к году за {month(lo)} - {month(end)} не рассчитана, нет полного набора сопоставимых значений.')
            if metric=='idx':
                a,b=valid[min(valid)],valid[max(valid)]
                add('index_change',label+': уровень индекса '+fmt(a,4)+' в '+month(min(valid))+' и '+fmt(b,4)+' в '+month(max(valid))+('; изменение уровня '+fmt((b/a-1)*100)+'%.' if a>0 else '; процент не определен.')+' Это отношение уровней индекса, не сумма индексов и не темп изменения массы; база индекса здесь не переопределяется.')
        # Calendar-month changes are computed, never inferred from neighboring rows.
        if info['type']=='fact' and metrics[0][0]!='idx':
            changes=[]
            for m,r in sorted(selected.items()):
                cols=[]
                for metric in ('stoim','netto'):
                    current=number(r.get(metric))
                    for lag in (1,12):
                        previous=lookup.get(m-lag,[])
                        b=number(previous[0].get(metric)) if len(previous)==1 else None
                        cols.append(fmt((current/b-1)*100) if current is not None and b is not None and b>0 else 'нет данных')
                changes.append('| '+month(m)+' | '+' | '.join(cols)+' |')
            add('monthly_changes',label+'\n\n| Месяц | Стоимость м/м, % | Стоимость г/г, % | Масса м/м, % | Масса г/г, % |\n|---|---:|---:|---:|---:|\n'+'\n'.join(changes)+'\nТемпы относятся к тем же календарным месяцам, источнику, типу и единицам. Неизвестная или неположительная база не заменяется нулем.')
        if metrics[0][0]!='idx':
            prices=[]
            for m,r in sorted(selected.items()):
                value,mass=number(r.get('stoim')),number(r.get('netto'))
                flags=str(r.get('quality_flags_raw',r.get('quality_flags',''))).lower()
                mass_flag=any(word in flags for word in ('netto','weight','масса','массы','массе','kg','кг'))
                price=value/mass*1000 if value is not None and mass is not None and mass>0 and not mass_flag else None
                prices.append('| '+month(m)+' | '+fmt(price)+' |')
            add('unit_value',label+'\n\n| Месяц | Средняя удельная стоимость, долл./т |\n|---|---:|\n'+'\n'.join(prices)+'\nРассчитано как стоимость / масса в тоннах. Это удельная стоимость поставок, не рыночная котировка. При неизвестной/неположительной массе или предупреждении о массе расчет не приводится. Флаг дополнительной единицы количества сам по себе не блокирует этот расчет.')
        header='| Месяц | '+' | '.join(t+', '+u for _,t,u,_ in metrics)+' |'
        table=[header,'|---|'+'---:|'*len(metrics)]
        for m in range(start,end+1):
            r=selected.get(m,{})
            table.append('| '+month(m)+' | '+' | '.join(fmt(number(r.get(k))/scale,3 if k=='idx' else 2) if number(r.get(k)) is not None else 'нет данных' for k,_,_,scale in metrics)+' |')
        add('monthly_table',label+'\n\n'+'\n'.join(table))
        if info['type']=='fact':
            # One compact table answers a monthly follow-up, including quantity YoY.
            dynmetrics=[m for m in metrics if m[0] in ('stoim','kol')]
            header='| Месяц | '+' | '.join(t+', '+u+' | Г/г, %' for _,t,u,_ in dynmetrics)+' |'
            dyn=[header,'|---|'+'---:|---:|'*len(dynmetrics)]
            for m in range(start,end+1):
                cols=[]
                for k,_,_,scale in dynmetrics:
                    current=number(selected.get(m,{}).get(k))
                    prev=lookup.get(m-12,[])
                    old=number(prev[0].get(k)) if len(prev)==1 else None
                    cols.extend([fmt(current/scale) if current is not None else 'нет данных',
                        fmt((current/old-1)*100,1) if current is not None and old is not None and old>0 else 'нет данных'])
                dyn.append('| '+month(m)+' | '+' | '.join(cols)+' |')
            if dynmetrics:add('monthly_dynamics',label+'\n\n'+'\n'.join(dyn))
    return facts

TOKEN=re.compile(r'\{\{fact:([a-f0-9]{16})\}\}')

def verify_answer(draft, facts):
    ids=TOKEN.findall(draft);unknown=sorted(set(ids)-set(facts));literal=TOKEN.sub('',draft)
    errors=[]
    if unknown:errors.append('Unknown fact references: '+','.join(unknown))
    # Known commodity codes and list numbering are labels, not numeric conclusions.
    known_codes=set(re.findall(r'код (\d+)', '\n'.join(v['text'] for v in facts.values())))
    literal=re.sub(r'(?i)((?:код|позици[яи])\s+)(\d+)',lambda m:m[1] if m[2] in known_codes else m[0],literal)
    literal=re.sub(r'(?m)^\s*\d+[.)]\s+', '', literal)
    if re.search(r'\d',literal):errors.append('Unverified digits outside fact references: '+str(re.findall(r'\d[\d ,.]*',literal)[:6]))
    if re.search(r'\b(?:вырос\w*|раст\w*|выше|ниже|вдвое|втрое|процент\w*|состав\w*|достиг\w*|сниз\w*|упал\w*|рост\w*|сократ\w*|максим\w*|миним\w*|увелич\w*|больш\w*|меньш\w*)',literal,re.I):errors.append('Unverified directional/extremum claim outside facts')
    if facts and not ids:errors.append('No verified facts selected')
    if '{{' in literal:errors.append('Malformed fact reference')
    return {'valid':not errors,'errors':errors,'answer':compact_text(TOKEN.sub(lambda m:facts.get(m[1],{}).get('text',''),draft),facts) if not errors else None}


def compact_text(text, facts):
    # Preserve all values and period labels, remove only exact repeated prefixes.
    labels=set()
    for fact in facts.values():
        match=re.match(r'(.+?; окно \d{4}-\d{2} - \d{4}-\d{2})',fact['text'])
        if match:labels.add(match[1])
    # A country/source label is essential when several slices share one answer.
    if len(labels)>1:return text
    for label in sorted(labels,key=len,reverse=True):
        first=text.find(label)
        if first>=0:
            head=text[:first+len(label)]
            rest=text[first+len(label):].replace(label+'. ','').replace(label+': ','')
            text=head+rest
    return text

def ranking_facts(result, filters):
    """Render server-computed full-denominator ranking as verifiable evidence."""
    ranking=result.get('ranking',{})
    if not ranking.get('complete_universe'):return {}
    facts={}
    def add(kind,text):
        facts[hashlib.sha256(text.encode()).hexdigest()[:16]]={'kind':kind,'text':text}
    countries=filters.get('strana') or []
    if isinstance(countries,str):countries=[countries]
    label=', '.join(COUNTRIES.get(c,c) for c in countries) or 'Выбранные страны'
    direction=filters.get('napr');direction=direction[0] if isinstance(direction,list) else direction
    route='Импорт России' if direction in ('im','ИМ') else 'Экспорт России'
    period=ranking.get('requested_period') or [filters.get('period_from',''),filters.get('period_to','')]
    total=number(ranking['total_stoim']);scale,unit=display_scale('stoim',[total])
    add('ranking_headline',f'**{route}, {label}: {str(period[0])[:7]} - {str(period[1])[:7]}.**\n\nСтоимость по доступным фактическим записям: **{fmt(total/scale)} {unit}**. Доли рассчитаны по всем товарным группам выбранного среза.')
    data=result.get('data',[])
    if data:
        leader=data[0]
        name=leader.get('tnved2_name') or 'название отсутствует'
        if name.isupper():name=name.capitalize()
        add('ranking_leader',f"Главная товарная группа: {name} ({leader['tnved2']}), **{fmt(leader['share_pct'],1)}%** доступной стоимости.")
    table=[f'| Код | Товарная группа | {unit} | Доля, % |','|---|---|---:|---:|']
    for row in data:
        name=str(row.get('tnved2_name') or 'Название отсутствует').replace('|',' ').replace('\n',' ')
        table.append(f"| {row['tnved2']} | {name.capitalize() if name.isupper() else name} | {fmt(number(row['stoim'])/scale)} | {fmt(row['share_pct'],1)} |")
    add('ranking_table','\n'.join(table))
    add('ranking_coverage',f"Остальные группы: {fmt(number(ranking['other_groups_stoim'])/scale)} {unit.rstrip('.')}. Месяцы с записями: {', '.join(ranking.get('fact_months',[]))}. Это итог доступных записей выбранного периода."+
        (' Месяцы без записей: '+', '.join(ranking['missing_months'])+'. Пропуски не заменены нулями.' if ranking.get('missing_months') else ''))
    return facts
