"""TNVED normalization and reference loading helpers."""

from core.normalization_rules import add_tnved_columns, normalize_tnved_code
from core.reference_tables import load_tnved_mapping


def generate_derived_columns(df):
    """Backward-compatible alias for TNVED normalization and derived columns."""
    return add_tnved_columns(df)


__all__ = [
    "add_tnved_columns",
    "generate_derived_columns",
    "load_tnved_mapping",
    "normalize_tnved_code",
]
