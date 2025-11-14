# China Country Code Fix

## Issue Summary

China data from Comtrade was appearing in the unified database even though national China data was present, violating the rule that Comtrade data should only be used for countries without national data.

## Root Cause

The issue was caused by **inconsistent country codes** for China:

1. **china-collector.py** was setting `STRANA = 'CH'` (incorrect)
2. **china_processor.py** was setting `STRANA = 'CN'` (correct)
3. The processed file `ch_full.parquet` contained `STRANA = 'CH'`

### Why This Caused the Problem

In the Comtrade partner mapping (metadata/comtrate-partnerAreas.json):
- **'CN'** = China (M49 code: 156) ✓
- **'CH'** = Switzerland (M49 code: 756/757) ✗

When the merge script ran:
1. It collected `'CH'` from the national data as the country to exclude
2. It mapped `'CH'` to Switzerland's M49 code (756/757)
3. It excluded Switzerland from Comtrade queries
4. **China (CN, M49: 156) was NOT excluded**, so Comtrade China data appeared in output

## Changes Made

### 1. Fixed china-collector.py
Changed line 162 from:
```python
df['STRANA'] = 'CH'
```
to:
```python
df['STRANA'] = 'CN'  # ISO 3166-1 alpha-2 code for China
```

### 2. Fixed china_processor.py
Changed line 316 from:
```python
output_file = project_root / 'data_processed' / 'ch_full.parquet'
```
to:
```python
output_file = project_root / 'data_processed' / 'cn_full.parquet'  # Using ISO 3166-1 alpha-2 code
```

### 3. Improved merge_processed_data.py logging
Added better logging to show which countries are being excluded from Comtrade data.

## Action Required

**You must regenerate the processed China data:**

1. Delete the old file:
   ```powershell
   Remove-Item data_processed\ch_full.parquet
   ```

2. Run the China processor:
   ```powershell
   python src/collectors/china_processor.py
   ```

3. This will create `data_processed\cn_full.parquet` with correct `STRANA = 'CN'`

4. Re-run the merge script:
   ```powershell
   python src/merge_processed_data.py --include-comtrade --start-year 2024
   ```

5. Verify that CN data only appears with `SOURCE = 'national'`, not `SOURCE = 'comtrade'`

## ISO 3166-1 Alpha-2 Country Codes

For reference, the correct ISO codes used in this project:
- **CN** = China
- **IN** = India  
- **TR** = Turkey (Türkiye)
- **CH** = Switzerland (not used in this project)

These codes must match the Comtrade partner mapping for proper exclusion logic.

