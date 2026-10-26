import csv
import hashlib
import json

import pandas as pd
import pytest

from collectors.china_evidence import ingest_export


def source(tmp_path, period='202608',partner='344'):
    p=tmp_path/'downloadData.csv'
    with p.open('w',encoding='utf-8-sig',newline='') as stream:
        writer=csv.writer(stream)
        writer.writerow(['Date of data','Trading partner code','Commodity code','US dollar',
                         'Quantity','Unit','Supplementary Quantity','Supplementary Unit','remarks'])
        writer.writerow([period,partner,'27101992','1,234','10','Kilogram','0','-','保密 / original note'])
    return p


def test_original_notes_and_parameters_preserved(tmp_path):
    src=source(tmp_path); payload=src.read_bytes()
    snapshot=b'{"status":"captured","fields":{"year":[{"value":"2026"}]}}'
    src.with_suffix('.query.json').write_bytes(snapshot)
    dest,archive=ingest_export(src,tmp_path/'output',2026,8,'ИМ')
    assert src.read_bytes()==payload==(archive/'original.csv').read_bytes()
    meta=json.loads((archive/'manifest.json').read_text(encoding='utf-8'))
    assert meta['sha256']==hashlib.sha256(payload).hexdigest()
    assert meta['requested']['china_flow']=='ИМ'
    assert meta['flow_verification']=='requested_only'
    assert meta['absence_semantics']=='unknown'
    assert meta['query_snapshot_preserved'] is True
    assert (archive/'query_snapshot.json').read_bytes()==snapshot
    data=pd.read_csv(dest)
    assert data.remarks.iloc[0]=='保密 / original note'
    assert data.STOIM.iloc[0]==1234 and data.NAPR.iloc[0]=='ЭК'


@pytest.mark.parametrize('field,value', [('Date of data','202607'),('Trading partner code','999')])
def test_every_row_validated_before_overwrite(tmp_path,field,value):
    src=source(tmp_path);output=tmp_path/'output'
    dest,_=ingest_export(src,output,2026,8,'ИМ')
    previous=dest.read_bytes()
    d=pd.read_csv(src,dtype=str);d=pd.concat([d,d],ignore_index=True);d.loc[1,field]=value
    d.to_csv(src,index=False)
    with pytest.raises(ValueError):ingest_export(src,output,2026,8,'ИМ')
    assert dest.read_bytes()==previous
    metas=[json.loads(p.read_text(encoding='utf-8')) for p in (output/'originals').glob('*/manifest.json')]
    assert sorted(m['validation_status'] for m in metas)==['rejected','validated']


def test_malformed_csv_archived_not_skipped(tmp_path):
    src=source(tmp_path)
    with src.open('a',encoding='utf-8') as stream:stream.write('broken,row,extra\n')
    with pytest.raises(ValueError,match='Malformed'):ingest_export(src,tmp_path/'output',2026,8,'ИМ')
    assert next((tmp_path/'output/originals').glob('*/original.csv')).read_bytes()==src.read_bytes()
    assert not (tmp_path/'output/EXPORT').exists()


def test_previous_processed_revision_preserved(tmp_path):
    src=source(tmp_path);output=tmp_path/'output'
    dest,_=ingest_export(src,output,2026,8,'ИМ');previous=dest.read_bytes()
    _,archive=ingest_export(src,output,2026,8,'ИМ')
    assert (archive/'previous_processed.csv').read_bytes()==previous
