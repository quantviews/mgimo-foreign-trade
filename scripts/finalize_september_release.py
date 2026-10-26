"""Assemble the locally recalculated September candidate, never publish here."""
import shutil
import sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from pipelines.merge_pipeline import parse_merge_args,resolve_merge_paths,run_merge_pipeline

out=ROOT/'tmp/release-20261024'
files=list((out/'fisher').glob('fizob_*.parquet'))
assert len(files)==4 and (out/'nowcast/nowcast.parquet').exists()
for f in files: shutil.copy2(f,out/'processed'/f.name)
p=resolve_merge_paths(output_db_path=str(out/'unified_trade_data.duckdb'))
p.update(data_processed_dir=out/'processed',comtrade_db_path=out/'comtrade.db',nowcast_path=out/'nowcast/nowcast.parquet')
assert not p['output_db_path'].exists()
run_merge_pipeline(parse_merge_args(['--include-comtrade','--start-year','2019']),p)
assert p['output_db_path'].exists()
