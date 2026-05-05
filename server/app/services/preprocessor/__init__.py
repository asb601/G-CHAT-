"""Preprocessor sub-package.

Contains the type-detection registry used by data_preprocessor.
Public API:
    detect_column_converter(col_name, sample) -> ColumnConverter | None
"""
from app.services.preprocessor.type_detection import (
    ColumnConverter,
    DEFAULT_REGISTRY,
    TypeDetectionRegistry,
    detect_column_converter,
)

__all__ = [
    "ColumnConverter",
    "DEFAULT_REGISTRY",
    "TypeDetectionRegistry",
    "detect_column_converter",
]
