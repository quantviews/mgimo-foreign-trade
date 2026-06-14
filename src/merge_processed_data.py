#!/usr/bin/env python3
"""Backward-compatible entrypoint for the merge pipeline.

The implementation is split across ``src/core`` and ``src/pipelines`` modules.
This file keeps existing imports and CLI usage working:

    python src/merge_processed_data.py
"""

from core.duckdb_writer import save_to_duckdb
from core.edizm import load_common_edizm_mapping, load_edizm_mapping
from core.normalization_rules import (
    add_tnved_columns,
    apply_special_edizm_cases,
    get_special_edizm_aliases,
    normalize_edizm_value,
    normalize_tnved_code,
    resolve_edizm_record,
    resolve_edizm_records,
    standardize_edizm_columns,
)
from core.reference_tables import (
    load_partner_mapping,
    load_strana_mapping,
    save_reference_tables,
)
from core.schema import (
    EXPECTED_SCHEMA,
    load_and_validate_file,
    smoke_check_merged_dataset,
    validate_schema,
)
from core.tnved import generate_derived_columns, load_tnved_mapping
from pipelines.merge_pipeline import (
    load_and_transform_comtrade,
    main,
    transform_fizob_to_unified,
)
from pipelines.nowcast_ingest import transform_nowcast_to_unified

__all__ = [
    "EXPECTED_SCHEMA",
    "add_tnved_columns",
    "apply_special_edizm_cases",
    "generate_derived_columns",
    "get_special_edizm_aliases",
    "load_and_transform_comtrade",
    "load_and_validate_file",
    "load_common_edizm_mapping",
    "load_edizm_mapping",
    "load_partner_mapping",
    "load_strana_mapping",
    "load_tnved_mapping",
    "main",
    "normalize_edizm_value",
    "normalize_tnved_code",
    "resolve_edizm_record",
    "resolve_edizm_records",
    "save_reference_tables",
    "save_to_duckdb",
    "smoke_check_merged_dataset",
    "standardize_edizm_columns",
    "transform_fizob_to_unified",
    "transform_nowcast_to_unified",
    "validate_schema",
]


if __name__ == "__main__":
    main()
