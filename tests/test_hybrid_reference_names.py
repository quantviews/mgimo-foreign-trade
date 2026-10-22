from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from core.reference_tables import load_tnved_mapping

def test_hybrid_names_exact_levels_only(reference_fixture):
 root=reference_fixture
 m=load_tnved_mapping(root)
 for code in ('870340','870350','870360','870370'):
  parent=m['tnved6'][code]
  assert parent['source']=='manual' and not parent['translated']
  assert 'СМ3' not in parent['name'] and 'ЭЛЕКТРОДВИГАТЕЛЕМ' in parent['name']
  assert ('БЕЗ ВОЗМОЖНОСТИ' in parent['name'])==(code in ('870340','870350'))
  for level in (8,10):assert m[f'tnved{level}'][code.ljust(level,'0')]['name']==parent['name']
 # Narrow national codes keep their own displacement and usage constraints.
 assert '1000' in m['tnved10']['8703401010']['name']
 assert m['tnved10']['8703401010']['source']=='fts'
 assert 'ГИБРИД' not in m['tnved6']['870322']['name']
