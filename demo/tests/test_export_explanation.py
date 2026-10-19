import asyncio,json
from demo.app import tools

FILTERS=dict(strana=['CN'],napr=['ИМ'],tnved4=['8703'],period_from='2025-09-01',period_to='2026-08-01')

def test_exact_export_window_and_scope():
    args=tools._export_explanation_args(FILTERS)
    assert args['months']==12 and args['period_to']=='2026-08-01' and args['code']=='8703'
    assert tools._export_explanation_args(dict(FILTERS,strana=['CN','IN'])) is None
    assert tools._export_explanation_args(dict(FILTERS,edizm=['796'])) is None
    assert tools._export_explanation_args(dict(FILTERS,source=['national','comtrade'])) is None

def test_attached_without_second_export(monkeypatch):
    ctx=dict(wants_export=True,answer='download',completed_export={'id':'one'})
    async def fake(client,args,tool_name):
        assert ctx['wants_export'] is False and tool_name=='explain_trade'
        return json.dumps({'available':True,'children':[{'code':'870340','share_pct':30}]})
    monkeypatch.setattr(tools,'_compare_trade',fake)
    asyncio.run(tools._attach_export_explanation(None,FILTERS,ctx))
    assert ctx['analysis_evidence']['explanation']['available']
    assert ctx['wants_export'] and ctx['answer']=='download' and ctx['completed_export']=={'id':'one'}

def test_optional_analysis_failure_keeps_file(monkeypatch):
    ctx=dict(wants_export=True,answer='download')
    async def fake(*a,**kw):
        ctx.update(error_code='comparison_failed',answer='failure')
        return json.dumps({'error':'failed'})
    monkeypatch.setattr(tools,'_compare_trade',fake)
    asyncio.run(tools._attach_export_explanation(None,FILTERS,ctx))
    assert ctx['answer']=='download' and 'error_code' not in ctx
