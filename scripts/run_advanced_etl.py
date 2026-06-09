"""
Command-line runner for the Advanced ETL pipeline.

This script executes the full OpenAlex ETL workflow:

1. EXTRACT: retrieve raw records from OpenAlex through the API.
2. TRANSFORM: convert raw JSON records into the WoS-like schema.
3. CALCULATED FIELDS: generate SR through the existing Bibliometrix-Python formatter.
4. VALIDATION: check mandatory columns, null values and type contracts.
5. LOAD: export the standardized DataFrame to CSV.

Example:

python scripts/run_advanced_etl.py --platform openalex --query "machine learning" --max-results 50 --output standardized_openalex.csv
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
SERVICES_DIR = REPO_ROOT / "www" / "services"
sys.path.insert(0, str(SERVICES_DIR))


from api_retriever import fetch_openalex_works
from mappings import MULTI_VALUE_COLUMNS
from standardizer import standardize_openalex_records
from validators import validate_standardized_dataframe


def extract_records(
    platform: str,
    query: str,
    max_results: int,
) -> List[Dict[str, Any]]:
    """
    Dispatcher for the EXTRACT phase.

    Args:
        platform: Selected bibliographic platform.
        query: Textual query provided by the user.
        max_results: Maximum number of records to retrieve.

    Returns:
        Raw records retrieved from the selected platform.
    """
    platform = platform.lower().strip()

    if platform == "openalex":
        return fetch_openalex_works(
            query=query,
            max_results=max_results,
        )

    raise ValueError(f"Unsupported platform: {platform}")


def transform_records(
    platform: str,
    records: List[Dict[str, Any]],
) -> pd.DataFrame:
    """
    Dispatcher for the TRANSFORM phase.

    Args:
        platform: Selected bibliographic platform.
        records: Raw records retrieved during the extract phase.

    Returns:
        Standardized WoS-like DataFrame.
    """
    platform = platform.lower().strip()

    if platform == "openalex":
        return standardize_openalex_records(records)

    raise ValueError(f"Unsupported platform: {platform}")


def serialize_lists_for_csv(df: pd.DataFrame) -> pd.DataFrame:
    """
    Serialize list-valued columns into semicolon-delimited strings.

    During transformation and validation, multi-value fields are kept as list[str].
    For flat CSV export, they are converted to strings using ';' as delimiter.
    """
    output_df = df.copy()

    for column in MULTI_VALUE_COLUMNS:
        output_df[column] = output_df[column].apply(
            lambda values: "; ".join(values) if isinstance(values, list) else str(values)
        )

    return output_df


def save_standardized_csv(df: pd.DataFrame, output_path: str) -> None:
    """
    Save the standardized DataFrame as a UTF-8 CSV file.
    """
    output_df = serialize_lists_for_csv(df)
    output_df.to_csv(output_path, index=False, encoding="utf-8")


def run_pipeline(
    platform: str,
    query: str,
    max_results: int,
    output_path: str,
) -> pd.DataFrame:
    """
    Run the complete Advanced ETL pipeline.
    """
    print("=== ADVANCED BIBLIOMETRIX ETL ===")
    print(f"Platform: {platform}")
    print(f"Query: {query}")
    print(f"Max results: {max_results}")

    print("\n[1/5] EXTRACT - Retrieving raw API records...")
    raw_records = extract_records(
        platform=platform,
        query=query,
        max_results=max_results,
    )
    print(f"Raw records retrieved: {len(raw_records)}")

    print("\n[2/5] TRANSFORM - Standardizing records into WoS-like schema...")
    standardized_df = transform_records(
        platform=platform,
        records=raw_records,
    )
    print(f"Standardized rows: {len(standardized_df)}")

    print("\n[3/5] CALCULATED FIELDS - SR generated during standardization.")
    print("SR sample:")
    print(standardized_df[["TI", "SR"]].head(3).to_string())

    print("\n[4/5] VALIDATION - Checking schema, null values and type contracts...")
    validate_standardized_dataframe(standardized_df)
    print("VALIDATION PASSED")

    print("\n[5/5] LOAD - Saving standardized CSV...")
    save_standardized_csv(standardized_df, output_path)
    print(f"Output saved to: {output_path}")

    print("\nNormalized preview:")
    preview_columns = ["DB", "UT", "DI", "TI", "PY", "TC", "AU", "SO", "SR"]
    print(standardized_df[preview_columns].head().to_string())

    return standardized_df


def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Run the Advanced Bibliometrix ETL pipeline."
    )

    parser.add_argument(
        "--platform",
        required=True,
        choices=["openalex"],
        help="Open-access platform to query.",
    )

    parser.add_argument(
        "--query",
        required=True,
        help='Textual search query, for example "machine learning".',
    )

    parser.add_argument(
        "--max-results",
        type=int,
        default=50,
        help="Maximum number of records to retrieve.",
    )

    parser.add_argument(
        "--output",
        default="standardized_openalex.csv",
        help="Output CSV path.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()

    run_pipeline(
        platform=args.platform,
        query=args.query,
        max_results=args.max_results,
        output_path=args.output,
    )