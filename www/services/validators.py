"""
Validation module for the Advanced ETL pipeline.

This module implements the VALIDATION phase required by the project.
It checks that the standardized DataFrame follows the WoS-like schema,
contains no null values, and respects the expected type contracts.
"""

from __future__ import annotations

from typing import List
from numbers import Integral

import pandas as pd

try:
    from .mappings import (
        INTEGER_COLUMNS,
        MULTI_VALUE_COLUMNS,
        STANDARD_COLUMNS,
        STRING_COLUMNS,
    )
except ImportError:
    from mappings import (
        INTEGER_COLUMNS,
        MULTI_VALUE_COLUMNS,
        STANDARD_COLUMNS,
        STRING_COLUMNS,
    )


class ValidationError(ValueError):
    """Raised when the standardized DataFrame violates the target schema."""


def validate_mandatory_columns(df: pd.DataFrame) -> None:
    """
    Verify that all mandatory WoS-like columns exist in the DataFrame.
    """
    missing_columns: List[str] = [
        column
        for column in STANDARD_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
        raise ValidationError(
            f"Missing mandatory columns: {missing_columns}"
        )


def validate_no_null_values(df: pd.DataFrame) -> None:
    """
    Verify that the DataFrame contains no pandas NaN values.
    """
    if df.isna().any().any():
        columns_with_nulls = df.columns[df.isna().any()].tolist()
        raise ValidationError(
            f"Null values found in columns: {columns_with_nulls}"
        )

    for column in df.columns:
        for index, value in df[column].items():
            if value is None:
                raise ValidationError(
                    f"None value found at row {index}, column {column}"
                )


def validate_multi_value_columns(df: pd.DataFrame) -> None:
    """
    Verify that multi-value fields are list[str].

    Multi-value columns such as AU, AF, C1, CR, DE and ID must be Python lists.
    Each element inside the list must be a string.
    """
    for column in MULTI_VALUE_COLUMNS:
        if column not in df.columns:
            continue

        for index, value in df[column].items():
            if not isinstance(value, list):
                raise ValidationError(
                    f"Column {column} at row {index} must be a Python list, "
                    f"got {type(value).__name__}"
                )

            for item in value:
                if not isinstance(item, str):
                    raise ValidationError(
                        f"Column {column} at row {index} must contain only strings. "
                        f"Invalid item type: {type(item).__name__}. "
                        f"Invalid item value: {repr(item)}"
                    )


def validate_string_columns(df: pd.DataFrame) -> None:
    """
    Verify that scalar string fields are strings.
    """
    for column in STRING_COLUMNS:
        if column not in df.columns:
            continue

        for index, value in df[column].items():
            if not isinstance(value, str):
                raise ValidationError(
                    f"Column {column} at row {index} must be str, "
                    f"got {type(value).__name__}"
                )


def validate_integer_columns(df: pd.DataFrame) -> None:
    """
    Verify that integer fields are integers.
    """
    for column in INTEGER_COLUMNS:
        if column not in df.columns:
            continue

        for index, value in df[column].items():
            if not isinstance(value, int):
                raise ValidationError(
                    f"Column {column} at row {index} must be int, "
                    f"got {type(value).__name__}"
                )


def validate_standardized_dataframe(df: pd.DataFrame) -> None:
    """
    Run the complete validation suite on a standardized DataFrame.

    The function raises ValidationError if the DataFrame is invalid.
    If no exception is raised, the DataFrame is valid.
    """
    validate_mandatory_columns(df)
    validate_no_null_values(df)
    validate_multi_value_columns(df)
    validate_string_columns(df)
    validate_integer_columns(df)