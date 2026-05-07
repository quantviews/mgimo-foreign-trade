"""Reference table loaders and DuckDB reference-table writer."""

from pipelines.merge_pipeline import (
    load_partner_mapping,
    load_strana_mapping,
    save_reference_tables,
)

__all__ = [
    "load_partner_mapping",
    "load_strana_mapping",
    "save_reference_tables",
]
