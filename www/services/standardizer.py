"""
Standardization module for the Advanced ETL pipeline.

This module implements the TRANSFORM phase.
It converts raw OpenAlex JSON records into the WoS-like schema required by
Bibliometrix-Python.

The module does not retrieve data from APIs and does not save files.
It only transforms records and enforces basic type contracts.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

import pandas as pd

try:
    from .mappings import (
        DATABASE_SOURCE_LABELS,
        MULTI_VALUE_COLUMNS,
        OPENALEX_TO_WOS_MAPPING,
        STANDARD_COLUMNS,
    )
except ImportError:
    from mappings import (
        DATABASE_SOURCE_LABELS,
        MULTI_VALUE_COLUMNS,
        OPENALEX_TO_WOS_MAPPING,
        STANDARD_COLUMNS,
    )
try:
    from .format_functions import format_sr_column
except Exception:
    try:
        from format_functions import format_sr_column
    except Exception:
        format_sr_column = None

def is_missing(value: Any) -> bool:
    """
    Check whether a value is missing.

    This prevents None, NaN, empty strings and textual null values from reaching
    the final standardized DataFrame.
    """
    if value is None:
        return True

    try:
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        pass

    if isinstance(value, str) and value.strip().lower() in {
        "",
        "nan",
        "none",
        "null",
        "na",
        "n/a",
    }:
        return True

    return False


def clean_string(value: Any) -> str:
    """
    Convert a scalar value into a clean string.

    Missing scalar values are replaced by an empty string, as required by the
    project type contracts.
    """
    if is_missing(value):
        return ""

    if isinstance(value, list):
        return "; ".join(clean_string(item) for item in value if not is_missing(item))

    return str(value).strip()


def clean_doi(value: Any) -> str:
    """
    Normalize DOI values.

    OpenAlex often returns DOI values as URLs, for example:
    https://doi.org/10.xxxx/yyyy

    The standardized output keeps only the DOI code.
    """
    doi = clean_string(value)

    if doi.lower().startswith("https://doi.org/"):
        doi = doi[len("https://doi.org/") :]

    if doi.lower().startswith("http://doi.org/"):
        doi = doi[len("http://doi.org/") :]

    return doi.strip()


def clean_year(value: Any) -> str:
    """
    Extract a four-digit publication year.
    """
    text = clean_string(value)
    match = re.search(r"(19|20)\d{2}", text)
    return match.group(0) if match else ""


def clean_integer(value: Any) -> int:
    """
    Convert a value to integer.

    Invalid or missing values are converted to 0.
    """
    if is_missing(value):
        return 0

    try:
        return int(float(clean_string(value)))
    except ValueError:
        return 0


def ensure_list_of_strings(value: Any) -> List[str]:
    """
    Convert a value into list[str].

    This is used for multi-value fields such as AU, AF, C1, CR, DE and ID.
    """
    if is_missing(value):
        return []

    if isinstance(value, list):
        cleaned_items: List[str] = []

        for item in value:
            if isinstance(item, list):
                cleaned_items.extend(ensure_list_of_strings(item))
            elif isinstance(item, dict):
                item_string = clean_string(
                    item.get("id")
                    or item.get("display_name")
                    or item.get("title")
                    or item
                )
                if item_string:
                    cleaned_items.append(item_string)
            else:
                item_string = clean_string(item)
                if item_string:
                    cleaned_items.append(item_string)

        return cleaned_items

    text = clean_string(value)

    if not text:
        return []

    parts = re.split(r"\s*;\s*|\s*\|\s*|\n+", text)
    return [part.strip() for part in parts if part.strip()]


def reconstruct_abstract(abstract_inverted_index: Optional[Dict[str, List[int]]]) -> str:
    """
    Reconstruct an OpenAlex abstract from its inverted-index format.

    OpenAlex stores abstracts as:
    {
        "machine": [0],
        "learning": [1]
    }

    This function reconstructs:
    "machine learning"
    """
    if not abstract_inverted_index or not isinstance(abstract_inverted_index, dict):
        return ""

    positioned_words: Dict[int, str] = {}

    for word, positions in abstract_inverted_index.items():
        if not isinstance(positions, list):
            continue

        for position in positions:
            if isinstance(position, int):
                positioned_words[position] = word

    if not positioned_words:
        return ""

    return " ".join(positioned_words[index] for index in sorted(positioned_words))


def get_source_info(record: Dict[str, Any]) -> Dict[str, str]:
    """
    Extract journal/source information from an OpenAlex record.
    """
    primary_location = record.get("primary_location") or {}
    source = primary_location.get("source") or {}

    source_name = clean_string(source.get("display_name"))
    source_abbreviation = clean_string(source.get("abbreviated_title"))

    return {
        "SO": source_name,
        "JI": source_abbreviation,
    }


def get_biblio_info(record: Dict[str, Any]) -> Dict[str, str]:
    """
    Extract volume, issue and page information from the OpenAlex biblio object.
    """
    biblio = record.get("biblio") or {}

    return {
        "VL": clean_string(biblio.get("volume")),
        "IS": clean_string(biblio.get("issue")),
        "BP": clean_string(biblio.get("first_page")),
        "EP": clean_string(biblio.get("last_page")),
    }


def get_author_info(record: Dict[str, Any]) -> Dict[str, List[str]]:
    """
    Extract author names and affiliations from OpenAlex authorships.
    """
    authorships = record.get("authorships") or []

    authors_short: List[str] = []
    authors_full: List[str] = []
    affiliations: List[str] = []

    for authorship in authorships:
        author = authorship.get("author") or {}
        author_name = clean_string(author.get("display_name"))

        if author_name:
            authors_full.append(author_name)
            authors_short.append(author_name)

        institutions = authorship.get("institutions") or []

        for institution in institutions:
            institution_name = clean_string(institution.get("display_name"))
            country_code = clean_string(institution.get("country_code"))

            if institution_name and country_code:
                affiliations.append(f"{institution_name}, {country_code}")
            elif institution_name:
                affiliations.append(institution_name)

        raw_affiliation_strings = authorship.get("raw_affiliation_strings") or []

        for raw_affiliation in raw_affiliation_strings:
            raw_affiliation = clean_string(raw_affiliation)
            if raw_affiliation:
                affiliations.append(raw_affiliation)

    return {
        "AU": authors_short,
        "AF": authors_full,
        "C1": list(dict.fromkeys(affiliations)),
    }


def get_keyword_info(record: Dict[str, Any]) -> Dict[str, List[str]]:
    """
    Extract keywords and concepts from OpenAlex.

    In the standardized schema:
    - DE contains author-like keywords where available.
    - ID contains broader index/concept keywords.
    """
    keywords = record.get("keywords") or []
    concepts = record.get("concepts") or []

    author_keywords: List[str] = []
    index_keywords: List[str] = []

    for keyword in keywords:
        keyword_text = clean_string(
            keyword.get("display_name")
            or keyword.get("keyword")
            or keyword.get("name")
        )

        if keyword_text:
            author_keywords.append(keyword_text)

    for concept in concepts:
        concept_name = clean_string(concept.get("display_name"))

        if concept_name:
            index_keywords.append(concept_name)

    return {
        "DE": list(dict.fromkeys(author_keywords)),
        "ID": list(dict.fromkeys(index_keywords)),
    }


def create_empty_standard_record() -> Dict[str, Any]:
    """
    Create an empty record with all mandatory standard columns.
    """
    record: Dict[str, Any] = {}

    for column in STANDARD_COLUMNS:
        if column in MULTI_VALUE_COLUMNS:
            record[column] = []
        elif column == "TC":
            record[column] = 0
        else:
            record[column] = ""

    return record


def standardize_openalex_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert one raw OpenAlex record into the WoS-like target schema.
    """
    standardized = create_empty_standard_record()

    standardized["DB"] = DATABASE_SOURCE_LABELS["openalex"]

    for openalex_field, wos_field in OPENALEX_TO_WOS_MAPPING.items():
        raw_value = record.get(openalex_field)

        if wos_field == "DI":
            standardized[wos_field] = clean_doi(raw_value)
        elif wos_field == "PY":
            standardized[wos_field] = clean_year(raw_value)
        elif wos_field == "TC":
            standardized[wos_field] = clean_integer(raw_value)
        elif wos_field in MULTI_VALUE_COLUMNS:
            standardized[wos_field] = [
                clean_string(item)
                for item in ensure_list_of_strings(raw_value)
                if clean_string(item)
            ]
        else:
            standardized[wos_field] = clean_string(raw_value)

    source_info = get_source_info(record)
    standardized.update(source_info)

    biblio_info = get_biblio_info(record)
    standardized.update(biblio_info)

    author_info = get_author_info(record)
    standardized.update(author_info)

    keyword_info = get_keyword_info(record)
    standardized.update(keyword_info)

    standardized["AB"] = reconstruct_abstract(record.get("abstract_inverted_index"))

    if format_sr_column is not None:
        standardized["SR"] = clean_string(
            format_sr_column(
                standardized,
                source="OpenAlex",
                file_type=".json",
            )
        )
    return standardized


def standardize_openalex_records(records: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Convert a list of raw OpenAlex records into a standardized DataFrame.
    """
    standardized_records = [
        standardize_openalex_record(record)
        for record in records
    ]

    df = pd.DataFrame(standardized_records)

    for column in STANDARD_COLUMNS:
        if column not in df.columns:
            if column in MULTI_VALUE_COLUMNS:
                df[column] = [[] for _ in range(len(df))]
            elif column == "TC":
                df[column] = 0
            else:
                df[column] = ""

    df = df[STANDARD_COLUMNS]

    for column in MULTI_VALUE_COLUMNS:
        df[column] = df[column].apply(ensure_list_of_strings)

    return df