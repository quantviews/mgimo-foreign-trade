#!/usr/bin/env python3
"""
Contract tests for the four data processor modules.

A contract test verifies that the processor output satisfies the unified
schema expected by merge_processed_data.py:
  - All 12 required columns are present
  - NAPR ∈ {'ИМ', 'ЭК'}
  - PERIOD is datetime64
  - STOIM, NETTO, KOL are numeric (float)
  - TNVED2/4/6 are consistent length-2/4/6 prefixes of TNVED
"""
import sys
import pytest
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "collectors"))

import load_fts_csv
import china_processor
import india_processor
import turkey_processor


REQUIRED_COLUMNS = frozenset({
    "NAPR", "PERIOD", "STRANA", "TNVED",
    "EDIZM", "EDIZM_ISO", "STOIM", "NETTO", "KOL",
    "TNVED2", "TNVED4", "TNVED6",
})


def assert_output_contract(df: pd.DataFrame, *, expected_strana: str = None) -> None:
    """Shared output-contract assertions for all processor DataFrames."""
    assert not df.empty, "Output DataFrame must not be empty"

    missing = REQUIRED_COLUMNS - set(df.columns)
    assert not missing, f"Missing required columns: {missing}"

    invalid_napr = set(df["NAPR"].dropna().unique()) - {"ИМ", "ЭК"}
    assert not invalid_napr, f"Invalid NAPR values: {invalid_napr}"

    assert pd.api.types.is_datetime64_any_dtype(df["PERIOD"]), (
        f"PERIOD must be datetime64, got {df['PERIOD'].dtype}"
    )

    for col in ("STOIM", "NETTO", "KOL"):
        assert pd.api.types.is_numeric_dtype(df[col]), (
            f"{col} must be numeric, got {df[col].dtype}"
        )

    assert df["TNVED"].dtype == object, "TNVED must be string (object dtype)"

    assert (df["TNVED2"] == df["TNVED"].str[:2]).all(), (
        "TNVED2 is not consistent with the first 2 chars of TNVED"
    )
    assert (df["TNVED4"] == df["TNVED"].str[:4]).all(), (
        "TNVED4 is not consistent with the first 4 chars of TNVED"
    )
    assert (df["TNVED6"] == df["TNVED"].str[:6]).all(), (
        "TNVED6 is not consistent with the first 6 chars of TNVED"
    )

    if expected_strana is not None:
        assert (df["STRANA"] == expected_strana).all(), (
            f"Expected STRANA='{expected_strana}', got: {df['STRANA'].unique()}"
        )


# ---------------------------------------------------------------------------
# china_processor
# ---------------------------------------------------------------------------

class TestChinaProcessorContract:
    """Contract tests for china_processor.process_and_merge_china_data.

    Input layout: raw_data_dir/IMPORT/data*.csv + raw_data_dir/EXPORT/data*.csv
    PERIOD in CSV: 'YYYY-MM' (processor appends '-01')
    STRANA: overwritten to 'CN' unconditionally
    TNVED: zero-padded to 8 digits
    """

    @pytest.fixture
    def china_df(self, tmp_path):
        import_dir = tmp_path / "IMPORT"
        export_dir = tmp_path / "EXPORT"
        import_dir.mkdir()
        export_dir.mkdir()

        (import_dir / "data_2024_01.csv").write_text(
            "NAPR,PERIOD,STRANA,TNVED,TNVED2,TNVED4,TNVED6,STOIM,NETTO,KOL\n"
            "ИМ,2024-01,CN,01010100,01,0101,010101,5000.0,1000.0,5.0\n",
            encoding="utf-8",
        )
        (export_dir / "data_2024_01.csv").write_text(
            "NAPR,PERIOD,STRANA,TNVED,TNVED2,TNVED4,TNVED6,STOIM,NETTO,KOL\n"
            "ЭК,2024-01,CN,87042100,87,8704,870421,12000.0,8000.0,3.0\n",
            encoding="utf-8",
        )

        output = tmp_path / "cn_test.parquet"
        china_processor.process_and_merge_china_data(tmp_path, output)
        return pd.read_parquet(output)

    def test_required_columns_present(self, china_df):
        assert_output_contract(china_df, expected_strana="CN")

    def test_napr_both_directions(self, china_df):
        assert set(china_df["NAPR"].unique()) == {"ИМ", "ЭК"}

    def test_strana_hardcoded_cn(self, china_df):
        """STRANA is overwritten to 'CN' regardless of what the source CSV contains."""
        assert (china_df["STRANA"] == "CN").all()

    def test_tnved_zfill_8(self, china_df):
        """China TNVED codes are zero-padded to 8 digits."""
        assert (china_df["TNVED"].str.len() == 8).all()

    def test_period_is_datetime(self, china_df):
        """PERIOD is built from 'YYYY-MM' in CSV by appending '-01'."""
        assert pd.api.types.is_datetime64_any_dtype(china_df["PERIOD"])
        assert (china_df["PERIOD"].dt.year == 2024).all()
        assert (china_df["PERIOD"].dt.month == 1).all()


# ---------------------------------------------------------------------------
# india_processor
# ---------------------------------------------------------------------------

class TestIndiaProcessorContract:
    """Contract tests for india_processor.process_and_merge_india_data.

    Input: india_*.csv files with Year and Month columns (PERIOD is constructed).
    STOIM: multiplied ×1000 to normalize from source units to project standard.
    EDIZM_ISO: mapped via EDIZM_TO_ISO dict (e.g. KGS→166, NOS→796).
    """

    @pytest.fixture
    def india_df(self, tmp_path):
        (tmp_path / "india_2024_01.csv").write_text(
            "NAPR,STRANA,TNVED,TNVED2,TNVED4,TNVED6,STOIM,NETTO,KOL,EDIZM,Year,Month\n"
            "ИМ,IN,0101010000,01,0101,010101,5.0,1000.0,5.0,KGS,2024,1\n"
            "ЭК,IN,8704210000,87,8704,870421,12.0,8000.0,3.0,NOS,2024,1\n",
            encoding="utf-8",
        )
        # Pass a non-existent edizm file → edizm_rus_mapping = {}
        # → EDIZM stays as original raw value, EDIZM_ISO comes from EDIZM_TO_ISO dict
        missing_edizm = tmp_path / "edizm_missing.csv"
        output = tmp_path / "in_test.parquet"
        india_processor.process_and_merge_india_data(tmp_path, output, missing_edizm)
        return pd.read_parquet(output)

    def test_required_columns_present(self, india_df):
        assert_output_contract(india_df)

    def test_napr_both_directions(self, india_df):
        assert set(india_df["NAPR"].unique()) == {"ИМ", "ЭК"}

    def test_stoim_scaled_by_1000(self, india_df):
        """STOIM is multiplied ×1000 to normalize to thousands USD."""
        im_stoim = india_df.loc[india_df["NAPR"] == "ИМ", "STOIM"].iloc[0]
        assert im_stoim == 5000.0, f"Expected 5.0 × 1000 = 5000.0, got {im_stoim}"

    def test_edizm_iso_mapped_from_dict(self, india_df):
        """EDIZM_ISO is derived from EDIZM_TO_ISO: KGS→166, NOS→796."""
        im_iso = india_df.loc[india_df["NAPR"] == "ИМ", "EDIZM_ISO"].iloc[0]
        ek_iso = india_df.loc[india_df["NAPR"] == "ЭК", "EDIZM_ISO"].iloc[0]
        assert im_iso == "166", f"KGS should map to ISO 166, got {im_iso}"
        assert ek_iso == "796", f"NOS should map to ISO 796, got {ek_iso}"

    def test_period_constructed_from_year_month(self, india_df):
        """PERIOD is built from Year and Month columns, not a pre-existing PERIOD column."""
        assert (india_df["PERIOD"].dt.year == 2024).all()
        assert (india_df["PERIOD"].dt.month == 1).all()


# ---------------------------------------------------------------------------
# turkey_processor
# ---------------------------------------------------------------------------

class TestTurkeyProcessorContract:
    """Contract tests for turkey_processor.harmonize_df.

    NOTE: The UNITS dict is defined inside the `if __name__ == '__main__':` block
    in turkey_processor.py and is therefore unavailable when the module is imported.
    The `turkey_df` fixture injects a minimal UNITS dict via monkeypatch to work
    around this. The proper fix is to move UNITS to module level.

    Direction logic: Turkey's 'Export Dollar' → Russia's import (ИМ);
                     Turkey's 'Import Dollar' → Russia's export (ЭК).
    STOIM: European number format ('5.000' = 5000.0) is parsed to float.
    """

    _UNITS = {
        "KG/ADET": ["796", "ШТУКА", "ШТ"],
        "-": ["?", "?", "?"],
    }

    # Minimal raw DataFrame as produced by build_for_year / table_clean.
    # Row 0: Export Dollar != '0' → NAPR='ИМ'
    # Row 1: Import Dollar != '0' → NAPR='ЭК'
    _RAW_DATA = {
        "Month":                    ["01",       "01"],
        "Country":                  ["RU",       "RU"],
        "HS8":                      ["01010100", "87042100"],
        "Unit":                     ["KG/ADET",  "KG/ADET"],
        "Country\xa0name":          ["Russia",   "Russia"],
        "HS8\xa0name":              ["Horses",   "Cars"],
        "Export\xa0Dollar":         ["5.000",    "0"],
        "Export\xa0quantity\xa01":  ["1.000",    "0"],
        "Export\xa0quantity\xa02":  ["5",        "0"],
        "Import\xa0Dollar":         ["0",        "12.000"],
        "Import\xa0quantity\xa01":  ["0",        "8.000"],
        "Import\xa0quantity\xa02":  ["0",        "3"],
    }

    @pytest.fixture
    def turkey_df(self, monkeypatch):
        monkeypatch.setattr(turkey_processor, "UNITS", self._UNITS, raising=False)
        raw = pd.DataFrame(self._RAW_DATA)
        return turkey_processor.harmonize_df(raw, "2024")

    def test_required_columns_present(self, turkey_df):
        assert_output_contract(turkey_df, expected_strana="TR")

    def test_napr_derived_from_dollar_columns(self, turkey_df):
        """NAPR is set from Export/Import Dollar columns, not from source data."""
        assert "ИМ" in turkey_df["NAPR"].values
        assert "ЭК" in turkey_df["NAPR"].values

    def test_strana_hardcoded_tr(self, turkey_df):
        assert (turkey_df["STRANA"] == "TR").all()

    def test_stoim_european_format_parsed(self, turkey_df):
        """'5.000' (European thousands separator) is correctly parsed as 5000.0."""
        im_stoim = turkey_df.loc[turkey_df["NAPR"] == "ИМ", "STOIM"].iloc[0]
        assert im_stoim == 5000.0, f"Expected 5000.0, got {im_stoim}"

    def test_units_dict_must_be_at_module_level(self):
        """UNITS is currently in __main__ block and unavailable on import (known bug).
        Once UNITS is moved to module level, remove the monkeypatch from turkey_df
        fixture and delete this test.
        """
        import importlib
        fresh = importlib.reload(turkey_processor)
        # After a clean reload (without monkeypatch) UNITS should NOT be defined.
        # If this test starts failing, UNITS was moved to module level — great, clean up.
        assert not hasattr(fresh, "UNITS"), (
            "UNITS is now at module level. Remove monkeypatch from turkey_df fixture."
        )


# ---------------------------------------------------------------------------
# load_fts_csv
# ---------------------------------------------------------------------------

class TestFtsCsvContract:
    """Contract tests for load_fts_csv.load_fts_csv_files.

    FTS_DIR is a module-level constant; we patch it to point to a temp dir.
    NAPR normalization: 'IMPORT'→'ИМ', 'EXPORT'→'ЭК' (and numeric '1'/'2').
    PERIOD is extracted from the filename pattern 'YYYY-MM.csv'.
    TNVED is right-padded to exactly 10 digits.
    """

    @pytest.fixture
    def fts_df(self, tmp_path, monkeypatch):
        (tmp_path / "2021-01.csv").write_text(
            "NAPR,STRANA,TNVED,STOIM,NETTO,KOL,EDIZM\n"
            "IMPORT,CN,0101010000,5000.0,1000.0,5.0,КГ\n"
            "EXPORT,IN,8704210000,12000.0,8000.0,3.0,ШТ\n",
            encoding="utf-8",
        )
        monkeypatch.setattr(load_fts_csv, "FTS_DIR", tmp_path)
        return load_fts_csv.load_fts_csv_files()

    @pytest.mark.xfail(
        strict=True,
        reason="load_fts_csv does not produce EDIZM_ISO — contract gap to fix",
    )
    def test_required_columns_present(self, fts_df):
        assert_output_contract(fts_df)

    def test_schema_minus_edizm_iso(self, fts_df):
        """FTS satisfies the unified contract for all columns except EDIZM_ISO."""
        fts_required = REQUIRED_COLUMNS - {"EDIZM_ISO"}
        missing = fts_required - set(fts_df.columns)
        assert not missing, f"Missing columns: {missing}"

        invalid_napr = set(fts_df["NAPR"].dropna().unique()) - {"ИМ", "ЭК"}
        assert not invalid_napr

        assert pd.api.types.is_datetime64_any_dtype(fts_df["PERIOD"])

        for col in ("STOIM", "NETTO", "KOL"):
            assert pd.api.types.is_numeric_dtype(fts_df[col])

        assert (fts_df["TNVED2"] == fts_df["TNVED"].str[:2]).all()
        assert (fts_df["TNVED4"] == fts_df["TNVED"].str[:4]).all()
        assert (fts_df["TNVED6"] == fts_df["TNVED"].str[:6]).all()

    def test_edizm_iso_not_produced(self, fts_df):
        """Documents that EDIZM_ISO is absent in FTS output (known gap).
        When this test starts failing, remove xfail from test_required_columns_present.
        """
        assert "EDIZM_ISO" not in fts_df.columns

    def test_napr_normalized_from_english(self, fts_df):
        """'IMPORT' → 'ИМ', 'EXPORT' → 'ЭК'."""
        assert set(fts_df["NAPR"].unique()) == {"ИМ", "ЭК"}

    def test_period_extracted_from_filename(self, fts_df):
        """PERIOD is extracted from filename '2021-01.csv', not from CSV content."""
        assert (fts_df["PERIOD"].dt.year == 2021).all()
        assert (fts_df["PERIOD"].dt.month == 1).all()

    def test_tnved_right_padded_to_10(self, fts_df):
        """TNVED is always exactly 10 characters, right-padded with zeros."""
        assert (fts_df["TNVED"].str.len() == 10).all()

    def test_numeric_napr_codes_normalized(self, tmp_path, monkeypatch):
        """'1' → 'ИМ' and '2' → 'ЭК' (FTS numeric NAPR codes)."""
        (tmp_path / "2021-02.csv").write_text(
            "NAPR,STRANA,TNVED,STOIM,NETTO,KOL,EDIZM\n"
            "1,CN,0101010000,5000.0,1000.0,5.0,КГ\n"
            "2,IN,8704210000,12000.0,8000.0,3.0,ШТ\n",
            encoding="utf-8",
        )
        monkeypatch.setattr(load_fts_csv, "FTS_DIR", tmp_path)
        df = load_fts_csv.load_fts_csv_files()
        assert set(df["NAPR"].unique()) == {"ИМ", "ЭК"}


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
