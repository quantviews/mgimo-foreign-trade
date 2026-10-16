from io import BytesIO
import duckdb
from openpyxl import load_workbook
from app.query import build_trade_query
from app.export_utils import build_export


def test_odata_estimate_filter_distinguishes_false_and_unknown():
    from app.odata import filter_to_sql
    assert filter_to_sql('KOL_ESTIMATED eq true') == ('KOL_ESTIMATED = ?', [True])
    assert filter_to_sql('NETTO_ESTIMATED eq false') == ('NETTO_ESTIMATED = ?', [False])
    assert filter_to_sql('NETTO_ESTIMATED eq null') == ('NETTO_ESTIMATED IS NULL', [])


def test_source_estimates_and_unknown_mass_survive_aggregation_and_excel():
    con = duckdb.connect()
    con.execute("""CREATE TABLE unified_trade_data AS
      SELECT 'BA' STRANA, 'fact' AS TYPE, '8703' TNVED4, '8703230000' TNVED,
        '796' EDIZM_ISO, 'ШТУКА' EDIZM, 100.0 STOIM, 20.0 NETTO,
        0.1 KOL, true KOL_ESTIMATED, true NETTO_ESTIMATED,
        '' PHYSICAL_QUALITY
      UNION ALL SELECT 'BA','fact','8703','8703230000','796','ШТУКА',
        200,NULL,0.2,true,NULL,'Масса неизвестна'""")
    sql, args, _ = build_trade_query(filters={}, group_by=['strana','edizm'],
        metrics=['stoim','netto','kol'],include=[],period_from=None,
        period_to=None,order_by=None,limit=10,offset=0)
    cur=con.execute(sql,args)
    row=dict(zip([c[0] for c in cur.description],cur.fetchone()))
    assert row['stoim']==300 and row['netto'] is None
    assert float(row['kol'])==0.3
    assert 'Количество оценено источником' in row['quality_flags']
    assert 'Масса оценена источником' in row['quality_flags']
    content,_,_=build_export([row],{}, {})
    wb=load_workbook(BytesIO(content)); values=list(wb['Данные'].values)
    record=dict(zip(values[0],values[1]))
    assert record['netto'] is None
    assert 'Comtrade' in record['quality_flags']
    assert any('SOURCE_QTY_ESTIMATED' in str(v) for r in wb['Описание'].values for v in r)
    # NULL standardized codes must not combine different source units.
    con.execute("UPDATE unified_trade_data SET EDIZM_ISO=NULL, EDIZM=CASE WHEN STOIM=100 THEN 'CTM' ELSE 'CRT' END")
    import pytest
    from app.query import QueryError
    with pytest.raises(QueryError):
        build_trade_query(filters={},group_by=['strana','edizm_iso'],
            metrics=['kol'],include=[],period_from=None,period_to=None,
            order_by=None,limit=10,offset=0)
    con.execute("UPDATE unified_trade_data SET EDIZM='?'")
    sql,args,_=build_trade_query(filters={},group_by=['strana','edizm'],
        metrics=['kol'],include=[],period_from=None,period_to=None,
        order_by=None,limit=10,offset=0)
    cur=con.execute(sql,args)
    mixed=dict(zip([c[0] for c in cur.description],cur.fetchone()))
    assert mixed['kol'] is None
    con.close()
