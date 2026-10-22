from pathlib import Path
import pandas as pd
from core.physical_units import select_comtrade_quantity, ensure_physical_provenance
from core.edizm import load_common_edizm_mapping
from core.normalization_rules import standardize_edizm_columns, apply_special_edizm_cases


def test_quantity_pairs_and_estimation_provenance():
    df = pd.DataFrame({
        'qtyUnitCode': [5, 5, 5, 8, 5], 'qty': [12, 3.5, 7, 90, 4],
        'altQtyUnitCode': [8, -1, 5, 5, 6], 'altQty': [100, 0, 0, 3, None],
        'STOIM': [100]*5, 'NETTO': [100, 0, None, 0, None],
        'SOURCE_QTY_ESTIMATED': [False, True, True, True, False],
        'SOURCE_NETTO_ESTIMATED': [False]*5,
    })
    out = select_comtrade_quantity(df, {5:'u',6:'pairs',8:'kg',-1:'N/A'})
    assert out.KOL.tolist() == [12, 3.5, 7, 3, 4]
    assert out.EDIZM_CODE.tolist() == [5]*5
    assert out.KOL_ESTIMATED.iloc[1]
    assert pd.isna(out.KOL_ESTIMATED.iloc[3])
    assert pd.isna(out.NETTO.iloc[1])
    assert out.NETTO.iloc[3] == 90 and out.NETTO_ESTIMATED.iloc[3]
    assert 'альтернативное' in out.PHYSICAL_QUALITY.iloc[2]


def test_india_units_preserved_and_ambiguous_not_guessed():
    df = pd.DataFrame({'STRANA':['IN']*5, 'SOURCE':['national']*5,
                       'EDIZM':['SQM','PRS','MTR','CBM','CTM'], 'KOL':[2.]*5})
    out = standardize_edizm_columns(df, load_common_edizm_mapping(Path('.')))
    assert out.EDIZM_ISO.iloc[:4].tolist() == ['055','715','006','113']
    assert out.EDIZM.iloc[4] == 'CTM' and pd.isna(out.EDIZM_ISO.iloc[4])
    assert out.SOURCE_UNIT.tolist() == df.EDIZM.tolist()


def test_tonne_recovery_does_not_create_zero_and_keeps_estimate():
    df = ensure_physical_provenance(pd.DataFrame({'EDIZM':['t']*2,
        'EDIZM_ISO':['168']*2, 'NETTO':[None,None], 'KOL':[0.,2.],
        'KOL_ESTIMATED':[False,True]}))
    out = apply_special_edizm_cases(df)
    assert pd.isna(out.NETTO.iloc[0])
    assert out.NETTO.iloc[1] == 2000 and out.NETTO_ESTIMATED.iloc[1]


def test_special_units_are_not_silently_pieces_or_mass():
    df = pd.DataFrame({'STRANA':['BA']*4, 'SOURCE':['comtrade']*4,
        'EDIZM':['12u','u (jeu/pack)','kg/net eda','ct/l'], 'KOL':[2.]*4,
        'NETTO':[None]*4})
    out = apply_special_edizm_cases(standardize_edizm_columns(df, load_common_edizm_mapping(Path('.'))))
    assert out.EDIZM.nunique() == 4
    assert out.EDIZM_ISO.isna().all()
    assert out.KOL.eq(2).all() and out.NETTO.isna().all()


def test_tonnes_do_not_displace_items_and_mass_has_separate_provenance():
    df=pd.DataFrame({'qtyUnitCode':[5], 'qty':[3.], 'altQtyUnitCode':[21],
        'altQty':[2.], 'STOIM':[100.], 'NETTO':[0.],
        'SOURCE_QTY_ESTIMATED':[True], 'SOURCE_NETTO_ESTIMATED':[False]})
    row=select_comtrade_quantity(df,{5:'u',21:'1000 KG',8:'kg',-1:'N/A'}).iloc[0]
    assert row.KOL==3 and row.EDIZM_CODE==5 and row.KOL_ESTIMATED
    assert row.NETTO==2000 and pd.isna(row.NETTO_ESTIMATED)
    assert not row.SOURCE_NETTO_ESTIMATED
