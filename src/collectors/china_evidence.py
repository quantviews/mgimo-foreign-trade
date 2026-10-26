"""Preserve original customs exports before parsing; never infer completeness."""
import csv
import hashlib
import io
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pandas as pd

from core.country_processor_contract import mirror_napr_value


def archive_export(source_file, output_dir, requested):
    payload = Path(source_file).read_bytes()
    folder = Path(output_dir) / 'originals' / (datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid4().hex[:12])
    folder.mkdir(parents=True, exist_ok=False)
    (folder/'original.csv').write_bytes(payload)
    query_file=Path(source_file).with_suffix('.query.json')
    if query_file.exists():
        (folder/'query_snapshot.json').write_bytes(query_file.read_bytes())
    manifest = dict(schema_version=1,source_url='https://stats.customs.gov.cn/indexEn',
        archived_at=datetime.now(timezone.utc).isoformat(), original_filename=Path(source_file).name,
        sha256=hashlib.sha256(payload).hexdigest(), requested=requested,
        parameter_provenance='caller_requested_not_independently_verified',
        validation_status='pending', publication_status='unknown', absence_semantics='unknown',
        query_snapshot_preserved=query_file.exists())
    save_manifest(folder,manifest)
    return folder,manifest,payload


def save_manifest(folder,manifest):
    (folder/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')


def decode_export(payload):
    for encoding in ['utf-8-sig','gb18030','ISO-8859-1']:
        try:
            return payload.decode(encoding),encoding
        except UnicodeDecodeError:
            continue


def ingest_export(source_file,output_dir,year,month,flow,partner_code='344'):
    requested = dict(year=str(year),month=str(month).zfill(2),china_flow=flow,
        partner_code=str(partner_code),currency='USD',frequency='monthly',commodity_scope='all')
    folder,manifest,payload = archive_export(source_file,output_dir,requested)
    try:
        if flow not in {'ИМ','ЭК'} or str(partner_code)!='344':
            raise ValueError('Russia pipeline requires partner 344 and a valid China flow')
        content,encoding = decode_export(payload)
        manifest['encoding'] = encoding
        records = list(csv.reader(io.StringIO(content),strict=True))
        records = [row for row in records if row]
        if not records or any(len(row)!=len(records[0]) for row in records[1:]):
            raise ValueError('Malformed CSV: inconsistent number of fields')
        if len(set(records[0])) != len(records[0]):
            raise ValueError('Duplicate CSV headers')
        data = pd.read_csv(io.StringIO(content),dtype=str,keep_default_na=False,on_bad_lines='error')
        required = {'Date of data','Trading partner code','Commodity code','US dollar',
                    'Quantity','Unit','Supplementary Quantity','Supplementary Unit'}
        if required-set(data):
            raise ValueError('Missing required source columns: '+str(sorted(required-set(data))))
        if data.empty:
            raise ValueError('Empty export does not establish zero trade')
        expected = f'{int(year):04d}{int(month):02d}'
        pd.Timestamp(f'{int(year):04d}-{int(month):02d}-01')
        if not data['Date of data'].str.strip().eq(expected).all():
            raise ValueError('Export contains unexpected periods')
        if not data['Trading partner code'].str.strip().eq(str(partner_code)).all():
            raise ValueError('Export contains unexpected partners')
        codes=data['Commodity code'].str.strip()
        if not codes.str.fullmatch(r'\d{8}').all():
            raise ValueError('Expected detailed 8-digit commodity export')
        value=pd.to_numeric(data['US dollar'].str.replace(',','',regex=False),errors='raise')
        if value.isna().any() or value.lt(0).any():
            raise ValueError('Invalid trade values')
        result=data.rename(columns={'Commodity code':'TNVED','US dollar':'STOIM'}).copy()
        result['TNVED']=codes
        result['STOIM']=value
        result['PERIOD']=f'{int(year):04d}-{int(month):02d}'
        result['STRANA']='CN'
        result['NAPR']=mirror_napr_value(flow)
        # Keep source partner, labels, remarks and all other supplied columns.
        destination=Path(output_dir)/('EXPORT' if flow=='ИМ' else 'IMPORT')/f'data{expected}.csv'
        destination.parent.mkdir(parents=True,exist_ok=True)
        # Preserve the previous processed revision before replacing it.
        if destination.exists():
            (folder/'previous_processed.csv').write_bytes(destination.read_bytes())
        staged=destination.with_name(destination.name+'.'+uuid4().hex+'.tmp')
        try:
            result.to_csv(staged,index=False,encoding='utf-8-sig')
            staged.replace(destination)
        finally:
            staged.unlink(missing_ok=True)
        manifest.update(validation_status='validated',rows=len(data),value_usd=float(value.sum()),
            observed_periods=sorted(data['Date of data'].unique().tolist()),
            observed_partners=sorted(data['Trading partner code'].unique().tolist()),
            source_columns=list(data),remarks_preserved='remarks' in data,
            flow_verification='requested_only',processed_file=str(destination))
        save_manifest(folder,manifest)
        return destination,folder
    except Exception as exc:
        manifest.update(validation_status='rejected',validation_error=str(exc))
        save_manifest(folder,manifest)
        raise
