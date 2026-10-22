import sys
from pathlib import Path
import pandas as pd
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from core.china_units import normalize_quantities
COMMON={'KILOGRAM':{'KOD':'166','NAME':'КИЛОГРАММ'},'NUMBER OF ITEM':{'KOD':'796','NAME':'ШТУКА'}}
def test_car_swaps_legacy_quantities():
 r=normalize_quantities(pd.DataFrame([dict(PERIOD='2026-08',TNVED='87031011',NETTO=10,KOL=12000,EDIZM='Kilogram')]),{(2026,'87031011'):('Number of item','Kilogram')},COMMON).iloc[0]
 assert r.NETTO==12000 and r.KOL==10 and r.EDIZM_ISO=='796'
def test_explicit_unit_and_no_year_fallback():
 rows=pd.DataFrame([{'PERIOD':'2025-01','TNVED':'01012100','NETTO':12}, {'PERIOD':'2026-01','TNVED':'01012100','Quantity':300,'Unit':'Kilogram','Supplementary Quantity':2,'Supplementary Unit':'Number of item'}])
 out=normalize_quantities(rows,{(2026,'01012100'):('Kilogram','Number of item')},COMMON)
 assert pd.isna(out.iloc[0].NETTO)
 assert out.iloc[1].NETTO==300 and out.iloc[1].KOL==2
def test_no_double_count_kg_as_supplementary_count():
 out=normalize_quantities(pd.DataFrame([dict(PERIOD='2026-01',TNVED='27090000',NETTO=100)]),{(2026,'27090000'):('Kilogram','')},COMMON)
 assert out.iloc[0].NETTO==100 and pd.isna(out.iloc[0].KOL)


def test_mass_conversion_and_energy_units():
    common={**COMMON, '246': {'KOD':'246','NAME':'ТЫСЯЧА КИЛОВАТТ-ЧАСОВ'}}
    rows=pd.DataFrame([dict(PERIOD='2026-01',TNVED='71081200',Quantity=2000,Unit='Gram'),dict(PERIOD='2026-01',TNVED='27160000',Quantity=5000,Unit='Kilowatt-hour')])
    out=normalize_quantities(rows,{},common)
    assert out.iloc[0].NETTO==2
    assert pd.isna(out.iloc[1].NETTO) and out.iloc[1].KOL==5 and out.iloc[1].EDIZM_ISO=='246'

def test_all_year_reference_files_are_read(tmp_path):
    from core.china_units import load_year_units
    for year in (2019,2026):
        (tmp_path/f'{year}-china.csv').write_text(f'code,name,year,unit1_code,unit1,unit2_code,unit2\n87031011,Cars,{year},1,Number of item,2,Kilogram\n',encoding='utf-8')
    mapping=load_year_units(tmp_path)
    assert mapping[2026,'87031011']==('Number of item','Kilogram')
    assert mapping[2019,'87031011']==('Number of item','Kilogram')
