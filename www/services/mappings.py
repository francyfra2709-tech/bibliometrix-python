"""
Mapping definitions for the Advanced ETL pipeline.

This module contains the target WoS-like schema and the OpenAlex-to-WoS
mapping rules. No extraction or transformation logic is executed here.
Keeping mappings separated makes the ETL pipeline easier to maintain
and avoids hardcoded transformations inside analytical functions.
"""

from __future__ import annotations

from typing import Dict, List, Set


STANDARD_COLUMNS: List[str] = [
    "DB",
    "UT",
    "DI",
    "PMID",
    "TI",
    "SO",
    "JI",
    "PY",
    "DT",
    "LA",
    "TC",
    "AU",
    "AF",
    "C1",
    "RP",
    "CR",
    "DE",
    "ID",
    "AB",
    "VL",
    "IS",
    "BP",
    "EP",
    "SR",
]


MULTI_VALUE_COLUMNS: Set[str] = {
    "AU",
    "AF",
    "C1",
    "CR",
    "DE",
    "ID",
}


STRING_COLUMNS: Set[str] = {
    "DB",
    "UT",
    "DI",
    "PMID",
    "TI",
    "SO",
    "JI",
    "PY",
    "DT",
    "LA",
    "RP",
    "AB",
    "VL",
    "IS",
    "BP",
    "EP",
    "SR",
}


INTEGER_COLUMNS: Set[str] = {
    "TC",
}


OPENALEX_TO_WOS_MAPPING: Dict[str, str] = {
    "id": "UT",
    "doi": "DI",
    "title": "TI",
    "display_name": "TI",
    "publication_year": "PY",
    "type": "DT",
    "language": "LA",
    "cited_by_count": "TC",
    "referenced_works": "CR",
}


OPENALEX_DERIVED_FIELDS: Dict[str, str] = {
    "database_source": "DB",
    "source_name": "SO",
    "source_abbreviation": "JI",
    "authors_short": "AU",
    "authors_full": "AF",
    "author_affiliations": "C1",
    "author_keywords": "DE",
    "index_keywords": "ID",
    "abstract_text": "AB",
    "volume": "VL",
    "issue": "IS",
    "first_page": "BP",
    "last_page": "EP",
    "short_reference": "SR",
}


DATABASE_SOURCE_LABELS: Dict[str, str] = {
    "openalex": "OPENALEX",
}