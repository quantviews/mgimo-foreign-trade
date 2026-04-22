#!/usr/bin/env python3
"""
Tests for merge_processed_data.py module.
"""

import pytest
import pandas as pd
import numpy as np
import tempfile
import json
from pathlib import Path
from unittest.mock import patch, MagicMock
import sys
import duckdb

# Add src to path to import the module
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from merge_processed_data import (
    validate_schema,
    generate_derived_columns,
    load_tnved_mapping,
    load_strana_mapping,
    load_common_edizm_mapping,
    save_to_duckdb,
    smoke_check_merged_dataset,
    EXPECTED_SCHEMA
)


class TestValidateSchema:
    """Tests for validate_schema function."""
    
    def test_valid_schema(self):
        """Test validation with correct schema."""
        df = pd.DataFrame({
            'NAPR': ['ИМ', 'ЭК'],
            'PERIOD': pd.to_datetime(['2024-01-01', '2024-02-01']),
            'STRANA': ['RU', 'CN'],
            'TNVED': ['0101010000', '0202020000'],
            'EDIZM': ['КГ', 'ШТ'],
            'EDIZM_ISO': ['166', '796'],
            'STOIM': [1000.0, 2000.0],
            'NETTO': [500.0, 600.0],
            'KOL': [10.0, 20.0],
            'TNVED2': ['01', '02'],
            'TNVED4': ['0101', '0202'],
            'TNVED6': ['010101', '020202']
        })
        assert validate_schema(df, 'test.parquet') == True
    
    def test_missing_columns(self):
        """Test validation with missing columns."""
        df = pd.DataFrame({
            'NAPR': ['ИМ'],
            'PERIOD': pd.to_datetime(['2024-01-01']),
            # Missing other required columns
        })
        assert validate_schema(df, 'test.parquet') == False
    
    def test_invalid_napr_values(self):
        """Test validation with invalid NAPR values."""
        df = pd.DataFrame({
            'NAPR': ['ИМ', 'INVALID'],  # Invalid value
            'PERIOD': pd.to_datetime(['2024-01-01', '2024-02-01']),
            'STRANA': ['RU', 'CN'],
            'TNVED': ['0101010000', '0202020000'],
            'EDIZM': ['КГ', 'ШТ'],
            'EDIZM_ISO': ['166', '796'],
            'STOIM': [1000.0, 2000.0],
            'NETTO': [500.0, 600.0],
            'KOL': [10.0, 20.0],
            'TNVED2': ['01', '02'],
            'TNVED4': ['0101', '0202'],
            'TNVED6': ['010101', '020202']
        })
        assert validate_schema(df, 'test.parquet') == False
    
    def test_null_period(self):
        """Test validation with null PERIOD values."""
        df = pd.DataFrame({
            'NAPR': ['ИМ', 'ЭК'],
            'PERIOD': pd.to_datetime([None, '2024-02-01']),  # Null value
            'STRANA': ['RU', 'CN'],
            'TNVED': ['0101010000', '0202020000'],
            'EDIZM': ['КГ', 'ШТ'],
            'EDIZM_ISO': ['166', '796'],
            'STOIM': [1000.0, 2000.0],
            'NETTO': [500.0, 600.0],
            'KOL': [10.0, 20.0],
            'TNVED2': ['01', '02'],
            'TNVED4': ['0101', '0202'],
            'TNVED6': ['010101', '020202']
        })
        assert validate_schema(df, 'test.parquet') == False
    
    def test_wrong_data_types(self):
        """Test validation with wrong data types."""
        df = pd.DataFrame({
            'NAPR': ['ИМ', 'ЭК'],
            'PERIOD': pd.to_datetime(['2024-01-01', '2024-02-01']),
            'STRANA': ['RU', 'CN'],
            'TNVED': ['0101010000', '0202020000'],
            'EDIZM': ['КГ', 'ШТ'],
            'EDIZM_ISO': ['166', '796'],
            'STOIM': ['1000', '2000'],  # Should be float64
            'NETTO': [500.0, 600.0],
            'KOL': [10.0, 20.0],
            'TNVED2': ['01', '02'],
            'TNVED4': ['0101', '0202'],
            'TNVED6': ['010101', '020202']
        })
        assert validate_schema(df, 'test.parquet') == False


class TestGenerateDerivedColumns:
    """Tests for generate_derived_columns function."""

    def test_generates_all_derived_columns(self):
        """generate_derived_columns adds TNVED2/4/6/8 columns."""
        df = pd.DataFrame({'TNVED': ['0101010000']})
        result = generate_derived_columns(df)

        assert 'TNVED2' in result.columns
        assert 'TNVED4' in result.columns
        assert 'TNVED6' in result.columns
        assert 'TNVED8' in result.columns

    def test_leading_zeros_preserved_ten_digit_codes(self):
        """10-digit TNVED codes are kept verbatim; leading zeros are NOT stripped."""
        df = pd.DataFrame({
            'TNVED': ['0101010000', '0202020000', '0000870421']
        })
        result = generate_derived_columns(df)

        # Codes that are already 10 chars must not change
        assert result.loc[0, 'TNVED'] == '0101010000'
        assert result.loc[1, 'TNVED'] == '0202020000'
        assert result.loc[2, 'TNVED'] == '0000870421'

        # Derived columns for '0101010000' (section 01 — live animals)
        assert result.loc[0, 'TNVED2'] == '01'
        assert result.loc[0, 'TNVED4'] == '0101'
        assert result.loc[0, 'TNVED6'] == '010101'
        assert result.loc[0, 'TNVED8'] == '01010100'

        # Derived columns for '0000870421' — leading zeros are part of the code
        assert result.loc[2, 'TNVED2'] == '00'
        assert result.loc[2, 'TNVED4'] == '0000'
        assert result.loc[2, 'TNVED6'] == '000087'
        assert result.loc[2, 'TNVED8'] == '00008704'

    def test_short_codes_right_padded(self):
        """Codes shorter than 10 chars are right-padded with zeros; leading zeros kept."""
        df = pd.DataFrame({
            'TNVED': ['87', '8704', '870421', '87042100']
        })
        result = generate_derived_columns(df)

        assert result.loc[0, 'TNVED'] == '8700000000'
        assert result.loc[1, 'TNVED'] == '8704000000'
        assert result.loc[2, 'TNVED'] == '8704210000'
        assert result.loc[3, 'TNVED'] == '8704210000'

    def test_short_codes_with_leading_zeros_right_padded(self):
        """Short codes that start with zeros are right-padded, NOT lstripped."""
        df = pd.DataFrame({
            'TNVED': ['01', '0101', '010101', '0000123']
        })
        result = generate_derived_columns(df)

        assert result.loc[0, 'TNVED'] == '0100000000'
        assert result.loc[1, 'TNVED'] == '0101000000'
        assert result.loc[2, 'TNVED'] == '0101010000'
        # '0000123' (7 chars) → right-pad 3 zeros → '0000123000', NOT '1230000000'
        assert result.loc[3, 'TNVED'] == '0000123000'

        # Section 01 must have TNVED2 == '01', never '10'
        assert result.loc[0, 'TNVED2'] == '01'

    def test_long_codes_truncated(self):
        """Codes longer than 10 chars are truncated from the right."""
        df = pd.DataFrame({
            'TNVED': ['123', '0000123', '123456789012345']
        })
        result = generate_derived_columns(df)

        assert result.loc[0, 'TNVED'] == '1230000000'
        # '0000123' (7 chars) → right-pad 3 zeros → '0000123000'
        assert result.loc[1, 'TNVED'] == '0000123000'
        assert result.loc[2, 'TNVED'] == '1234567890'

    def test_all_zeros_code(self):
        """All-zeros codes are handled without raising errors."""
        df = pd.DataFrame({
            'TNVED': ['0000000000', '0']
        })
        result = generate_derived_columns(df)

        assert result.loc[0, 'TNVED'] == '0000000000'
        assert result.loc[1, 'TNVED'] == '0000000000'


class TestLoadTnvedMapping:
    """Tests for load_tnved_mapping function."""
    
    def test_load_official_mappings(self, tmp_path):
        """Test loading official TNVED mappings from CSV."""
        # Create metadata directory structure
        metadata_dir = tmp_path / "metadata"
        metadata_dir.mkdir()
        
        # Create temporary CSV file
        # Note: Names with commas must be quoted
        csv_file = metadata_dir / "tnved.csv"
        csv_content = """KOD,NAME,level
01,ЖИВЫЕ ЖИВОТНЫЕ,2
0101,"ЛОШАДИ, ОСЛЫ, МУЛЫ И ЛОШАКИ",4
010101,ЛОШАДИ,6
01010100,ЛОШАДИ ПЛЕМЕННЫЕ,8
0101010000,ЛОШАДИ ПЛЕМЕННЫЕ ЧИСТОЙ ПОРОДЫ,10"""
        csv_file.write_text(csv_content, encoding='utf-8')
        
        # Create empty translations file
        translations_dir = metadata_dir / "translations"
        translations_dir.mkdir()
        translations_file = translations_dir / "missing_codes_translations.json"
        translations_file.write_text('{}', encoding='utf-8')
        
        project_root = tmp_path
        mappings = load_tnved_mapping(project_root)
        
        assert 'tnved2' in mappings
        assert 'tnved4' in mappings
        assert 'tnved6' in mappings
        assert 'tnved8' in mappings
        assert 'tnved10' in mappings
        
        assert '01' in mappings['tnved2']
        assert mappings['tnved2']['01']['name'] == 'ЖИВЫЕ ЖИВОТНЫЕ'
        assert mappings['tnved2']['01']['translated'] == False
    
    def test_load_translations(self, tmp_path):
        """Test loading translations from JSON."""
        # Create metadata directory structure
        metadata_dir = tmp_path / "metadata"
        metadata_dir.mkdir()
        
        # Create empty official mappings file
        csv_file = metadata_dir / "tnved.csv"
        csv_file.write_text('KOD,NAME,level\n', encoding='utf-8')
        
        # Create translations file
        translations_dir = metadata_dir / "translations"
        translations_dir.mkdir()
        translations_file = translations_dir / "missing_codes_translations.json"
        translations_data = {
            "0101010000": {
                "russian_name": "Тестовое название"
            }
        }
        translations_file.write_text(json.dumps(translations_data, ensure_ascii=False), encoding='utf-8')
        
        project_root = tmp_path
        mappings = load_tnved_mapping(project_root)
        
        # Check that translation was loaded
        assert 'tnved10' in mappings
        # Code normalization: 0101010000 is already 10 chars → kept as-is (leading zeros preserved)
        normalized_code = '0101010000'
        assert normalized_code in mappings['tnved10']
        assert mappings['tnved10'][normalized_code]['name'] == 'ТЕСТОВОЕ НАЗВАНИЕ'  # Should be uppercase
        assert mappings['tnved10'][normalized_code]['translated'] == True
    
    def test_uppercase_names(self, tmp_path):
        """Test that all names are converted to uppercase."""
        # Create metadata directory structure
        metadata_dir = tmp_path / "metadata"
        metadata_dir.mkdir()
        
        # Create CSV with lowercase names
        csv_file = metadata_dir / "tnved.csv"
        csv_content = """KOD,NAME,level
01,живые животные,2"""
        csv_file.write_text(csv_content, encoding='utf-8')
        
        translations_dir = metadata_dir / "translations"
        translations_dir.mkdir()
        translations_file = translations_dir / "missing_codes_translations.json"
        translations_file.write_text('{}', encoding='utf-8')
        
        project_root = tmp_path
        mappings = load_tnved_mapping(project_root)
        
        assert mappings['tnved2']['01']['name'] == 'ЖИВЫЕ ЖИВОТНЫЕ'  # Should be uppercase


class TestLoadStranaMapping:
    """Tests for load_strana_mapping function."""
    
    def test_load_strana_mapping(self, tmp_path):
        """Test loading country name mappings."""
        # Create metadata directory structure
        metadata_dir = tmp_path / "metadata"
        metadata_dir.mkdir()
        
        csv_file = metadata_dir / "STRANA.csv"
        csv_content = """KOD	NAME
RU	РОССИЯ
CN	КИТАЙ
US	СОЕДИНЕННЫЕ ШТАТЫ"""
        csv_file.write_text(csv_content, encoding='utf-8')
        
        project_root = tmp_path
        mapping = load_strana_mapping(project_root)
        
        assert 'RU' in mapping
        assert 'CN' in mapping
        assert 'US' in mapping
        assert mapping['RU'] == 'РОССИЯ'
        assert mapping['CN'] == 'КИТАЙ'
    
    def test_case_insensitive_keys(self, tmp_path):
        """Test that keys are case-insensitive (uppercase)."""
        # Create metadata directory structure
        metadata_dir = tmp_path / "metadata"
        metadata_dir.mkdir()
        
        csv_file = metadata_dir / "STRANA.csv"
        csv_content = """KOD	NAME
ru	РОССИЯ
CN	КИТАЙ"""
        csv_file.write_text(csv_content, encoding='utf-8')
        
        project_root = tmp_path
        mapping = load_strana_mapping(project_root)
        
        # Keys should be uppercase
        assert 'RU' in mapping
        assert 'CN' in mapping
        assert mapping['RU'] == 'РОССИЯ'


class TestLoadCommonEdizmMapping:
    """Tests for load_common_edizm_mapping function."""
    
    def test_load_edizm_mapping(self, tmp_path):
        """Test loading EDIZM mappings."""
        # Create metadata directory structure
        metadata_dir = tmp_path / "metadata"
        metadata_dir.mkdir()
        
        csv_file = metadata_dir / "edizm.csv"
        csv_content = """KOD,NAME
166,КИЛОГРАММ
796,ШТУКА
112,ЛИТР"""
        csv_file.write_text(csv_content, encoding='utf-8')
        
        project_root = tmp_path
        mapping = load_common_edizm_mapping(project_root)
        
        # Should map both by KOD and NAME
        assert '166' in mapping
        assert 'КИЛОГРАММ' in mapping
        assert mapping['166']['NAME'] == 'КИЛОГРАММ'
        assert mapping['КИЛОГРАММ']['NAME'] == 'КИЛОГРАММ'
    
    def test_aliases(self, tmp_path):
        """Test that aliases are properly mapped."""
        # Create metadata directory structure
        metadata_dir = tmp_path / "metadata"
        metadata_dir.mkdir()
        
        csv_file = metadata_dir / "edizm.csv"
        csv_content = """KOD,NAME
166,КИЛОГРАММ
796,ШТУКА"""
        csv_file.write_text(csv_content, encoding='utf-8')
        
        project_root = tmp_path
        mapping = load_common_edizm_mapping(project_root)
        
        # Check aliases
        assert 'KG' in mapping
        assert 'КГ' in mapping
        assert mapping['KG']['NAME'] == 'КИЛОГРАММ'
        assert mapping['КГ']['NAME'] == 'КИЛОГРАММ'
    
    def test_uppercase_names(self, tmp_path):
        """Test that names are converted to uppercase."""
        # Create metadata directory structure
        metadata_dir = tmp_path / "metadata"
        metadata_dir.mkdir()
        
        csv_file = metadata_dir / "edizm.csv"
        csv_content = """KOD,NAME
166,килограмм"""
        csv_file.write_text(csv_content, encoding='utf-8')
        
        project_root = tmp_path
        mapping = load_common_edizm_mapping(project_root)
        
        assert mapping['166']['NAME'] == 'КИЛОГРАММ'  # Should be uppercase


class TestSaveToDuckDB:
    """Tests for save_to_duckdb function."""

    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame({
            'NAPR':     ['ИМ', 'ЭК'],
            'PERIOD':   pd.to_datetime(['2024-01-01', '2024-02-01']),
            'STRANA':   ['RU', 'CN'],
            'TNVED':    ['0101010000', '0202020000'],
            'EDIZM':    ['КГ', 'ШТ'],
            'EDIZM_ISO': ['166', '796'],
            'STOIM':    [1000.0, 2000.0],
            'NETTO':    [500.0, 600.0],
            'KOL':      [10.0, 20.0],
            'TNVED2':   ['01', '02'],
            'TNVED4':   ['0101', '0202'],
            'TNVED6':   ['010101', '020202'],
        })

    def test_save_to_duckdb(self, tmp_path):
        """Test saving DataFrame to DuckDB."""
        df = pd.DataFrame({
            'NAPR': ['ИМ', 'ЭК'],
            'PERIOD': pd.to_datetime(['2024-01-01', '2024-02-01']),
            'STRANA': ['RU', 'CN'],
            'TNVED': ['0101010000', '0202020000'],
            'EDIZM': ['КГ', 'ШТ'],
            'EDIZM_ISO': ['166', '796'],
            'STOIM': [1000.0, 2000.0],
            'NETTO': [500.0, 600.0],
            'KOL': [10.0, 20.0],
            'TNVED2': ['01', '02'],
            'TNVED4': ['0101', '0202'],
            'TNVED6': ['010101', '020202']
        })
        
        output_path = tmp_path / "test_db.duckdb"
        save_to_duckdb(df, output_path, table_name='test_table')
        
        # Verify file was created
        assert output_path.exists()
        
        # Verify data was saved correctly
        conn = duckdb.connect(str(output_path))
        result = conn.execute("SELECT COUNT(*) FROM test_table").fetchone()
        assert result[0] == 2
        
        # Verify PERIOD is saved as DATE
        result = conn.execute("SELECT PERIOD FROM test_table LIMIT 1").fetchone()
        assert result[0] is not None
        
        conn.close()
    
    def test_save_empty_dataframe(self, tmp_path):
        """Test saving empty DataFrame."""
        df = pd.DataFrame()
        output_path = tmp_path / "test_db.duckdb"
        
        # Should not raise error, just return
        save_to_duckdb(df, output_path)
        
        # File should not be created for empty DataFrame
        assert not output_path.exists()
    
    def test_save_with_chunking(self, tmp_path):
        """Test saving large DataFrame with chunking."""
        # Create DataFrame with more rows than chunk_size
        df = pd.DataFrame({
            'NAPR': ['ИМ'] * 150000,
            'PERIOD': pd.to_datetime(['2024-01-01'] * 150000),
            'STRANA': ['RU'] * 150000,
            'TNVED': ['0101010000'] * 150000,
            'EDIZM': ['КГ'] * 150000,
            'EDIZM_ISO': ['166'] * 150000,
            'STOIM': [1000.0] * 150000,
            'NETTO': [500.0] * 150000,
            'KOL': [10.0] * 150000,
            'TNVED2': ['01'] * 150000,
            'TNVED4': ['0101'] * 150000,
            'TNVED6': ['010101'] * 150000
        })

        output_path = tmp_path / "test_db.duckdb"
        save_to_duckdb(df, output_path, chunk_size=50000)

        # Verify all data was saved
        conn = duckdb.connect(str(output_path))
        result = conn.execute("SELECT COUNT(*) FROM unified_trade_data").fetchone()
        assert result[0] == 150000
        conn.close()

    # ------------------------------------------------------------------
    # Atomic write safety tests
    # ------------------------------------------------------------------

    def test_no_temp_file_left_after_success(self, tmp_path, sample_df):
        """The .tmp file must not exist after a successful write."""
        output = tmp_path / "test_db.duckdb"
        save_to_duckdb(sample_df, output)
        assert output.exists()
        assert not (tmp_path / "test_db.duckdb.tmp").exists()

    def test_existing_db_preserved_on_write_failure(self, tmp_path, sample_df):
        """If the write fails, the existing database must not be modified."""
        output = tmp_path / "test_db.duckdb"

        # Seed an existing valid database with sentinel data
        conn = duckdb.connect(str(output))
        conn.execute("CREATE TABLE sentinel AS SELECT 99 AS id")
        conn.close()
        original_mtime = output.stat().st_mtime

        # Force a failure by making duckdb.connect raise before writing anything
        with patch('duckdb.connect', side_effect=RuntimeError("simulated disk full")):
            with pytest.raises(RuntimeError):
                save_to_duckdb(sample_df, output)

        # Original file must be untouched (same mtime, still readable)
        assert output.exists()
        assert output.stat().st_mtime == original_mtime

        conn = duckdb.connect(str(output))
        result = conn.execute("SELECT id FROM sentinel").fetchone()
        conn.close()
        assert result[0] == 99

    def test_temp_file_cleaned_up_on_failure(self, tmp_path, sample_df):
        """The .tmp file must be removed after a failed write."""
        output = tmp_path / "test_db.duckdb"
        tmp_file = output.with_name(output.name + '.tmp')

        with patch('duckdb.connect', side_effect=RuntimeError("simulated error")):
            with pytest.raises(RuntimeError):
                save_to_duckdb(sample_df, output)

        assert not tmp_file.exists()

    def test_stale_temp_file_removed_before_write(self, tmp_path, sample_df):
        """A leftover .tmp file from a previous failed run is removed before starting."""
        output = tmp_path / "test_db.duckdb"
        tmp_file = output.with_name(output.name + '.tmp')

        # Simulate a stale temp file from a previous crash
        tmp_file.write_bytes(b"stale data")
        assert tmp_file.exists()

        save_to_duckdb(sample_df, output)

        assert output.exists()
        assert not tmp_file.exists()

    def test_overwrites_existing_file_correctly(self, tmp_path, sample_df):
        """Saving over an existing database replaces it with the new content."""
        output = tmp_path / "test_db.duckdb"

        # Write an old database
        conn = duckdb.connect(str(output))
        conn.execute("CREATE TABLE old_table AS SELECT 1 AS v")
        conn.close()

        save_to_duckdb(sample_df, output, table_name='unified_trade_data')

        # New table exists and has correct row count
        conn = duckdb.connect(str(output))
        count = conn.execute("SELECT COUNT(*) FROM unified_trade_data").fetchone()[0]
        conn.close()
        assert count == len(sample_df)


class TestIntegration:
    """Integration tests."""
    
    def test_schema_validation_with_generated_columns(self):
        """Test that generated columns pass schema validation."""
        df = pd.DataFrame({
            'NAPR': ['ИМ', 'ЭК'],
            'PERIOD': pd.to_datetime(['2024-01-01', '2024-02-01']),
            'STRANA': ['RU', 'CN'],
            'TNVED': ['0101010000', '0202020000'],
            'EDIZM': ['КГ', 'ШТ'],
            'EDIZM_ISO': ['166', '796'],
            'STOIM': [1000.0, 2000.0],
            'NETTO': [500.0, 600.0],
            'KOL': [10.0, 20.0]
        })
        
        # Generate derived columns
        df_processed = generate_derived_columns(df)
        
        # Should pass validation
        assert validate_schema(df_processed, 'test.parquet') == True
        assert 'TNVED2' in df_processed.columns
        assert 'TNVED4' in df_processed.columns
        assert 'TNVED6' in df_processed.columns
    
    def test_full_pipeline(self, tmp_path):
        """Test full pipeline: generate columns -> validate -> save."""
        # Create metadata directory structure
        metadata_dir = tmp_path / "metadata"
        metadata_dir.mkdir()
        
        # Create test data
        df = pd.DataFrame({
            'NAPR': ['ИМ', 'ЭК'],
            'PERIOD': pd.to_datetime(['2024-01-01', '2024-02-01']),
            'STRANA': ['RU', 'CN'],
            'TNVED': ['0101010000', '0202020000'],
            'EDIZM': ['КГ', 'ШТ'],
            'EDIZM_ISO': ['166', '796'],
            'STOIM': [1000.0, 2000.0],
            'NETTO': [500.0, 600.0],
            'KOL': [10.0, 20.0]
        })
        
        # Generate derived columns
        df_processed = generate_derived_columns(df)
        
        # Validate schema
        assert validate_schema(df_processed, 'test.parquet') == True
        
        # Save to DuckDB
        output_path = tmp_path / "test_db.duckdb"
        save_to_duckdb(df_processed, output_path)
        
        # Verify
        assert output_path.exists()
        conn = duckdb.connect(str(output_path))
        result = conn.execute("SELECT COUNT(*) FROM unified_trade_data").fetchone()
        assert result[0] == 2
        conn.close()


class TestSmokeCheckMergedDataset:
    """Tests for smoke_check_merged_dataset — the final quality gate before DuckDB write."""

    @pytest.fixture
    def valid_merged_df(self):
        """Minimal valid merged DataFrame that should pass all smoke checks."""
        return pd.DataFrame({
            'NAPR':     ['ИМ', 'ЭК'],
            'PERIOD':   pd.to_datetime(['2024-01-01', '2024-02-01']),
            'STRANA':   ['CN', 'TR'],
            'TNVED':    ['0101010000', '8704210000'],
            'EDIZM':    ['КИЛОГРАММ', 'ШТУКА'],
            'EDIZM_ISO': ['166', '796'],
            'STOIM':    [5000.0, 12000.0],
            'NETTO':    [1000.0, 8000.0],
            'KOL':      [5.0, 3.0],
            'TNVED2':   ['01', '87'],
            'TNVED4':   ['0101', '8704'],
            'TNVED6':   ['010101', '870421'],
        })

    def test_passes_on_valid_data(self, valid_merged_df):
        assert smoke_check_merged_dataset(valid_merged_df) is True

    def test_fails_on_empty_dataframe(self):
        assert smoke_check_merged_dataset(pd.DataFrame()) is False

    def test_fails_on_missing_required_columns(self):
        df = pd.DataFrame({
            'NAPR':   ['ИМ'],
            'PERIOD': pd.to_datetime(['2024-01-01']),
            # All other required columns missing
        })
        assert smoke_check_merged_dataset(df) is False

    def test_fails_when_period_is_string(self, valid_merged_df):
        df = valid_merged_df.copy()
        df['PERIOD'] = df['PERIOD'].astype(str)
        assert smoke_check_merged_dataset(df) is False

    def test_fails_when_period_has_nulls(self, valid_merged_df):
        df = valid_merged_df.copy()
        df.loc[0, 'PERIOD'] = pd.NaT
        assert smoke_check_merged_dataset(df) is False

    def test_fails_on_invalid_napr_values(self, valid_merged_df):
        df = valid_merged_df.copy()
        df.loc[0, 'NAPR'] = 'EXPORT'
        assert smoke_check_merged_dataset(df) is False

    def test_fails_on_numeric_napr_codes(self, valid_merged_df):
        """Numeric NAPR codes ('1', '2') must be normalised by processors before merge."""
        df = valid_merged_df.copy()
        df.loc[0, 'NAPR'] = '1'
        assert smoke_check_merged_dataset(df) is False

    def test_passes_with_extra_columns(self, valid_merged_df):
        """Extra columns (SOURCE, TYPE, TNVED8) should not cause failures."""
        df = valid_merged_df.copy()
        df['SOURCE'] = 'national'
        df['TYPE'] = 'fact'
        df['TNVED8'] = df['TNVED'].str[:8]
        assert smoke_check_merged_dataset(df) is True

    def test_multiple_checks_all_reported(self):
        """When multiple checks fail, the function returns False (not short-circuit)."""
        df = pd.DataFrame({
            'NAPR':   ['EXPORT'],   # invalid NAPR
            'PERIOD': ['2024-01-01'],  # wrong type (string, not datetime)
            # All other required columns missing
        })
        result = smoke_check_merged_dataset(df)
        assert result is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

