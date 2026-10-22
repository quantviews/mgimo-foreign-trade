from pathlib import Path
import sys
import duckdb
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from core.reference_tables import load_tnved_mapping, refresh_country_chapters


def test_india_chapter_is_scoped_and_global_name_is_neutral(reference_fixture):
    root=reference_fixture
    mappings=load_tnved_mapping(root)
    assert 'ЮАН' not in mappings['tnved2']['98']['name']
    assert 'RMB' not in mappings['tnved2']['98']['name_en']
    with duckdb.connect() as conn:
        refresh_country_chapters(conn,root)
        rows=conn.execute('SELECT STRANA,TNVED_CODE,TNVED_NAME FROM tnved_country_chapters').fetchall()
        assert len(rows)==1 and rows[0][:2]==('IN','98')
        assert 'Проектный импорт' in rows[0][2]
        assert conn.execute("SELECT count(*) FROM tnved_country_chapters WHERE STRANA='CN'").fetchone()[0]==0
