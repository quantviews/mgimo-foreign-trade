"""Install checked local artifacts, preserving the previous working files."""
import json
import os
import shutil
from pathlib import Path

root=Path(__file__).resolve().parents[1]
release=root/'tmp/release-20261024'
assert (release/'release-quality.json').exists()
assert (release/'fizob_bulletin.validation.json').exists()
pairs=[(release/'unified_trade_data.duckdb',root/'db/unified_trade_data.duckdb'),
       (release/'comtrade.db',root/'db/comtrade.db'),
       (release/'processed/cn_full.parquet',root/'data_processed/cn_full.parquet'),
       (release/'nowcast/nowcast.parquet',root/'data_processed/nowcast/nowcast.parquet'),
       (release/'fizob_bulletin.parquet',root/'site/data/fizob_bulletin.parquet')]
pairs += [(p,root/'data_processed'/p.name) for p in sorted((release/'fisher').glob('fizob_*.parquet'))]
for src,dst in pairs:
    assert src.exists() and dst.resolve().is_relative_to(root)
    backup=release/'local-before'/dst.relative_to(root)
    if dst.exists():
        assert not backup.exists(),f'Backup already exists: {backup}'
        backup.parent.mkdir(parents=True,exist_ok=True)
        shutil.copy2(dst,backup)
    dst.parent.mkdir(parents=True,exist_ok=True)
    temp=dst.with_name(dst.name+'.release-20261024.new')
    assert not temp.exists()
    shutil.copy2(src,temp)
    os.replace(temp,dst)
(release/'local-installed.json').write_text(json.dumps([str(dst.relative_to(root)) for _,dst in pairs],indent=2),encoding='utf-8')
print('Installed checked local artifacts:',len(pairs))
