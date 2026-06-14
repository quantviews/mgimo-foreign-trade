#!/usr/bin/env python3
"""Tests for nowcast parquet ingest into the unified merge schema."""

import logging
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pipelines.nowcast_ingest import (
    NOWCAST_UNIFIED_COLUMNS,
    append_nowcast_data,
    drop_nowcast_rows_superseded_by_facts,
    transform_nowcast_to_unified,
)


def _sample_nowcast_raw(*, type_values=("pred", "pred", "fact")) -> pd.DataFrame:
    n = len(type_values)
    return pd.DataFrame(
        {
            "STRANA": (["CN", "IN", "TR"][:n]),
            "PERIOD": pd.to_datetime(["2025-06-01", "2024-01-01", "2026-03-01"][:n]),
            "TNVED": ["0101010000", "0202020000", "0303030000"][:n],
            "NAPR": ["ИМ", "ЭК", "ИМ"][:n],
            "TYPE": list(type_values),
            "STOIM": [100.0, 200.0, 300.0][:n],
            "NETTO": [10.0, 20.0, 30.0][:n],
        }
    )


class TestTransformNowcastToUnified:
    def test_keeps_only_pred_rows(self):
        result = transform_nowcast_to_unified(_sample_nowcast_raw())
        assert len(result) == 2
        assert (result["TYPE"] == "pred").all()

    def test_output_columns_match_unified_contract(self):
        result = transform_nowcast_to_unified(_sample_nowcast_raw(type_values=("pred",)))
        assert list(result.columns) == list(NOWCAST_UNIFIED_COLUMNS)
        assert result["TYPE"].iloc[0] == "pred"

    def test_generates_tnved_derived_columns(self):
        result = transform_nowcast_to_unified(_sample_nowcast_raw(type_values=("pred",)))
        row = result.iloc[0]
        assert row["TNVED2"] == "01"
        assert row["TNVED4"] == "0101"
        assert row["TNVED6"] == "010101"

    def test_normalizes_strana_and_napr(self):
        raw = _sample_nowcast_raw(type_values=("pred",))
        raw.loc[0, "STRANA"] = "cn"
        raw.loc[0, "NAPR"] = " ИМ "
        result = transform_nowcast_to_unified(raw)
        assert result.iloc[0]["STRANA"] == "CN"
        assert result.iloc[0]["NAPR"] == "ИМ"

    def test_filters_by_start_year(self):
        result = transform_nowcast_to_unified(_sample_nowcast_raw(), start_year=2025)
        assert len(result) == 1
        assert result.iloc[0]["STRANA"] == "CN"

    def test_missing_required_columns_returns_empty(self):
        raw = _sample_nowcast_raw(type_values=("pred",)).drop(columns=["NETTO"])
        result = transform_nowcast_to_unified(raw)
        assert result.empty

    def test_no_pred_rows_returns_empty(self):
        raw = _sample_nowcast_raw(type_values=("fact", "fact"))
        result = transform_nowcast_to_unified(raw)
        assert result.empty

    def test_nullable_measure_columns_are_empty(self):
        result = transform_nowcast_to_unified(_sample_nowcast_raw(type_values=("pred",)))
        row = result.iloc[0]
        assert pd.isna(row["EDIZM"])
        assert pd.isna(row["EDIZM_ISO"])
        assert pd.isna(row["KOL"])


class TestDropNowcastRowsSupersededByFacts:
    @pytest.fixture
    def test_logger(self):
        return logging.getLogger("test_nowcast_ingest")

    def test_drops_pred_when_fact_exists_for_same_cell(self, test_logger):
        merged = pd.DataFrame(
            {
                "PERIOD": pd.to_datetime(["2026-01-01", "2026-01-01"]),
                "STRANA": ["CN", "CN"],
                "TNVED": ["0101010000", "0101010000"],
                "NAPR": ["ИМ", "ИМ"],
                "TYPE": ["fact", "pred"],
                "STOIM": [1000.0, 900.0],
            }
        )
        result = drop_nowcast_rows_superseded_by_facts(merged, test_logger)
        assert len(result) == 1
        assert result.iloc[0]["TYPE"] == "fact"

    def test_keeps_pred_when_no_matching_fact(self, test_logger):
        merged = pd.DataFrame(
            {
                "PERIOD": pd.to_datetime(["2026-01-01", "2026-02-01"]),
                "STRANA": ["CN", "CN"],
                "TNVED": ["0101010000", "0101010000"],
                "NAPR": ["ИМ", "ИМ"],
                "TYPE": ["fact", "pred"],
                "STOIM": [1000.0, 900.0],
            }
        )
        result = drop_nowcast_rows_superseded_by_facts(merged, test_logger)
        assert len(result) == 2

    def test_matches_tnved_after_normalization(self, test_logger):
        merged = pd.DataFrame(
            {
                "PERIOD": pd.to_datetime(["2026-01-01", "2026-01-01"]),
                "STRANA": ["CN", "CN"],
                "TNVED": ["101010000", "1010100000"],
                "NAPR": ["ИМ", "ИМ"],
                "TYPE": ["fact", "pred"],
                "STOIM": [1000.0, 900.0],
            }
        )
        result = drop_nowcast_rows_superseded_by_facts(merged, test_logger)
        assert len(result) == 1


class TestAppendNowcastData:
    def test_appends_transformed_rows_from_parquet(self, tmp_path):
        raw = _sample_nowcast_raw(type_values=("pred", "pred"))
        parquet_path = tmp_path / "nowcast.parquet"
        raw.to_parquet(parquet_path)

        frames: list[pd.DataFrame] = []
        append_nowcast_data(
            frames,
            include_nowcast=True,
            nowcast_path=parquet_path,
            excluded_countries_upper=[],
        )

        assert len(frames) == 1
        assert len(frames[0]) == 2
        assert (frames[0]["SOURCE"] == "nowcast").all()

    def test_respects_exclude_countries(self, tmp_path):
        raw = _sample_nowcast_raw(type_values=("pred", "pred"))
        parquet_path = tmp_path / "nowcast.parquet"
        raw.to_parquet(parquet_path)

        frames: list[pd.DataFrame] = []
        append_nowcast_data(
            frames,
            include_nowcast=True,
            nowcast_path=parquet_path,
            excluded_countries_upper=["CN"],
        )

        assert len(frames) == 1
        assert set(frames[0]["STRANA"]) == {"IN"}

    def test_skips_when_disabled(self, tmp_path):
        raw = _sample_nowcast_raw(type_values=("pred",))
        parquet_path = tmp_path / "nowcast.parquet"
        raw.to_parquet(parquet_path)

        frames: list[pd.DataFrame] = []
        append_nowcast_data(
            frames,
            include_nowcast=False,
            nowcast_path=parquet_path,
            excluded_countries_upper=[],
        )
        assert frames == []

    def test_skips_when_file_missing(self, tmp_path):
        frames: list[pd.DataFrame] = []
        append_nowcast_data(
            frames,
            include_nowcast=True,
            nowcast_path=tmp_path / "missing.parquet",
            excluded_countries_upper=[],
        )
        assert frames == []
