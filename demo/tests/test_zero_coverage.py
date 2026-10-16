from demo.app.exports import coverage_evidence

def test_zero_row_is_observed_missing_month_is_not():
    r=coverage_evidence([dict(period='2026-07-01',type='fact',source='national',stoim=0,netto=None,kol=None)],dict(period_from='2026-07-01',period_to='2026-08-01'))
    assert r['observed_fact_months']==['2026-07']
    assert r['missing_fact_months']==['2026-08']
    assert r['zero_value_rows']==1 and r['missing_mass_rows']==1
