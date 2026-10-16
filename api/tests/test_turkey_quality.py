import duckdb
from app.query import build_trade_query
from app.export_utils import build_export
from io import BytesIO
from openpyxl import load_workbook


def test_unknown_component_and_suspect_record_reach_excel():
    con = duckdb.connect()
    con.execute("""create table unified_trade_data as
        select 'TR' STRANA, 'fact' AS TYPE, '8703' TNVED4, '8703101100' TNVED,
        '796' EDIZM_ISO, 'ШТУКА' EDIZM, 151.0 STOIM, 3.0 NETTO, 3.0 KOL
        union all select 'TR','fact','2799','2799999900',NULL,NULL,100,NULL,NULL""")
    con.execute("ALTER TABLE unified_trade_data ADD COLUMN PHYSICAL_QUALITY VARCHAR")
    con.execute("ALTER TABLE unified_trade_data ADD COLUMN NETTO_ESTIMATED BOOLEAN")
    con.execute("ALTER TABLE unified_trade_data ADD COLUMN KOL_ESTIMATED BOOLEAN")
    sql, args, _ = build_trade_query(filters={}, group_by=['strana'], metrics=['stoim','netto'],
        include=[],period_from=None,period_to=None,order_by=None,limit=100,offset=0)
    cur=con.execute(sql,args);row=dict(zip([d[0] for d in cur.description],cur.fetchone()))
    assert row['stoim']==251 and row['netto'] is None
    assert '100 кг' in row['quality_flags'] and 'Скрытая' in row['quality_flags']
    content,_,_=build_export([row],{}, {})
    sheet=load_workbook(BytesIO(content))['Данные']
    cells=dict(zip([c.value for c in sheet[1]],[c.value for c in sheet[2]]))
    assert cells['netto'] is None and cells['netto_tonnes'] is None
    assert '100 кг' in cells['quality_flags']
    con.close()
