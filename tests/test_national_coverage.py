import json,hashlib,logging
import pandas as pd
import pytest
from pipelines.national_coverage import drop_confirmed_national_nowcast

def setup(tmp_path):
 (tmp_path/'metadata').mkdir();(tmp_path/'raw.csv').write_bytes(b'confirmed release')
 record=dict(country='CN',month='2026-08',direction='ИМ',status='confirmed_complete',raw_file='raw.csv',raw_sha256=hashlib.sha256(b'confirmed release').hexdigest(),rows=1,stoim_usd=100)
 (tmp_path/'metadata/national_release_coverage.json').write_text(json.dumps({'releases':[record]}))
 return pd.DataFrame([dict(STRANA=c,PERIOD=p,NAPR=n,TYPE=t,SOURCE=s,STOIM=v,TNVED=k) for c,p,n,t,s,v,k in [
 ('CN','2026-08-01','ИМ','fact','national',100,'8703221000'),
 ('CN','2026-08-01','ИМ','pred','nowcast',5,'8703999900'),
 ('CN','2026-08-01','ЭК','pred','nowcast',6,'8703999900'),
 ('CN','2026-09-01','ИМ','pred','nowcast',7,'8703999900'),
 ('IN','2026-08-01','ИМ','pred','nowcast',8,'8703999900')]])

def test_only_confirmed_month_direction_removed(tmp_path):
 f=setup(tmp_path);r=drop_confirmed_national_nowcast(f,tmp_path,logging.getLogger())
 assert r.index.tolist()==[0,2,3,4]
 assert r.iloc[0].STOIM==100

def test_partial_fact_blocks_build(tmp_path):
 f=setup(tmp_path);f.loc[0,'STOIM']=50
 with pytest.raises(ValueError,match='Incomplete'):drop_confirmed_national_nowcast(f,tmp_path,logging.getLogger())

def test_changed_release_requires_confirmation(tmp_path):
 f=setup(tmp_path);(tmp_path/'raw.csv').write_bytes(b'revised')
 with pytest.raises(ValueError,match='reconfirm'):drop_confirmed_national_nowcast(f,tmp_path,logging.getLogger())

def test_no_manifest_keeps_forecast(tmp_path):
 f=pd.DataFrame({'TYPE':['pred']})
 assert drop_confirmed_national_nowcast(f,tmp_path,logging.getLogger()).equals(f)
