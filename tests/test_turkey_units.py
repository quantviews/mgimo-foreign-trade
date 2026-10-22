from pathlib import Path
import sys
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from core.turkey_units import normalize_turkey_missing
from core.normalization_rules import resolve_edizm_record
from core.normalization_rules import standardize_edizm_columns


def test_compact_turkish_units():
    for raw, code in [('KG/METRE','006'), ('KG/1000ADET','798'),
                      ('KG/100ADET','797'), ('KG/1000LITRE','130'),
                      ('KG/KARAT','162'), ('KG/LT-ALK%100','831')]:
        assert resolve_edizm_record(raw)['KOD'] == code


def test_missing_weight_does_not_change_value_or_suspect_positive_mass():
    frame = pd.DataFrame({'STOIM':[100,0,151], 'NETTO':[0,0,3],
                          'KOL':[0,0,3], 'EDIZM':['?','?','ШТУКА']})
    out = normalize_turkey_missing(frame)
    assert pd.isna(out.NETTO[0]) and pd.isna(out.KOL[0])
    assert out.NETTO[1] == 0 and out.NETTO[2] == 3
    assert out.STOIM.equals(frame.STOIM)


def test_second_normalization_preserves_resolved_turkey_labels():
    frame = pd.DataFrame({'STRANA':['TR','TR','TR'],
        'EDIZM':['ЛИТР ЧИСТОГО (100%) СПИРТА','CT-L','ЛИТР ЧИСТОГО (100%) СПИРТА'],
        'EDIZM_ISO':['831',None,'831']})
    out = standardize_edizm_columns(frame, {})
    assert out.EDIZM.equals(frame.EDIZM)
    assert out.EDIZM_ISO.tolist() == ['831',None,'831']
