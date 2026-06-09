"""
API retriever module for the Advanced ETL pipeline.

This module implements the EXTRACT phase.
It retrieves raw bibliographic records from OpenAlex using a textual query.

The output is intentionally raw JSON-like data.
No standardization is performed here.
"""

from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional

import requests


OPENALEX_WORKS_URL = "https://api.openalex.org/works"


class OpenAlexAPIError(RuntimeError):
    """Raised when the OpenAlex API request fails after all retries."""


def _build_openalex_params(
    query: str,
    cursor: str,
    per_page: int,
    api_key: Optional[str] = None,
    mailto: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build the parameters for the OpenAlex Works API.

    Args:
        query: Textual search query, for example "machine learning".
        cursor: Cursor used for pagination. The first cursor must be "*".
        per_page: Number of records requested in one API call.
        api_key: Optional OpenAlex API key.
        mailto: Optional email address for polite API usage.

    Returns:
        A dictionary containing the request parameters.
    """
    params: Dict[str, Any] = {
        "search": query,
        "cursor": cursor,
        "per_page": per_page,
        "select": ",".join(
            [
                "id",
                "doi",
                "title",
                "display_name",
                "publication_year",
                "publication_date",
                "type",
                "language",
                "cited_by_count",
                "authorships",
                "primary_location",
                "locations",
                "abstract_inverted_index",
                "concepts",
                "keywords",
                "referenced_works",
                "biblio",
            ]
        ),
    }

    if api_key:
        params["api_key"] = api_key

    if mailto:
        params["mailto"] = mailto

    return params


def _request_with_retries(
    url: str,
    params: Dict[str, Any],
    max_retries: int = 3,
    timeout: int = 30,
    backoff_seconds: float = 2.0,
) -> Dict[str, Any]:
    """
    Perform an HTTP GET request with retry logic.

    This function retries temporary failures, server errors, and rate-limit errors.

    Args:
        url: API endpoint.
        params: Request parameters.
        max_retries: Maximum number of retry attempts.
        timeout: Request timeout in seconds.
        backoff_seconds: Initial waiting time between retries.

    Returns:
        Parsed JSON response.

    Raises:
        OpenAlexAPIError: If the request fails after all retries.
    """
    last_error: Optional[Exception] = None

    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=timeout)

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                sleep_time = float(retry_after) if retry_after else backoff_seconds * (2 ** attempt)
                time.sleep(sleep_time)
                continue

            if 500 <= response.status_code < 600:
                time.sleep(backoff_seconds * (2 ** attempt))
                continue

            response.raise_for_status()
            return response.json()

        except requests.RequestException as exc:
            last_error = exc
            time.sleep(backoff_seconds * (2 ** attempt))

    raise OpenAlexAPIError(
        f"OpenAlex request failed after {max_retries} attempts: {last_error}"
    )


def fetch_openalex_works(
    query: str,
    max_results: int = 100,
    per_page: int = 100,
    api_key: Optional[str] = None,
    mailto: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Retrieve raw works from OpenAlex using cursor pagination.

    Args:
        query: Textual query inserted by the user.
        max_results: Maximum number of records to retrieve.
        per_page: Number of records per API call.
        api_key: Optional OpenAlex API key. If missing, the function reads OPENALEX_API_KEY.
        mailto: Optional email address. If missing, the function reads OPENALEX_MAILTO.

    Returns:
        A list of raw OpenAlex records.
    """
    if not query or not query.strip():
        raise ValueError("Query cannot be empty.")

    if max_results <= 0:
        raise ValueError("max_results must be greater than 0.")

    if per_page <= 0 or per_page > 100:
        raise ValueError("per_page must be between 1 and 100.")

    api_key = api_key or os.getenv("OPENALEX_API_KEY")
    mailto = mailto or os.getenv("OPENALEX_MAILTO")

    records: List[Dict[str, Any]] = []
    cursor = "*"

    while len(records) < max_results:
        current_per_page = min(per_page, max_results - len(records))

        params = _build_openalex_params(
            query=query,
            cursor=cursor,
            per_page=current_per_page,
            api_key=api_key,
            mailto=mailto,
        )

        payload = _request_with_retries(
            OPENALEX_WORKS_URL,
            params=params,
        )

        page_results = payload.get("results", [])

        if not page_results:
            break

        records.extend(page_results)

        meta = payload.get("meta", {})
        cursor = meta.get("next_cursor")

        if not cursor:
            break

        time.sleep(0.2)

    return records[:max_results]


if __name__ == "__main__":
    works = fetch_openalex_works("machine learning", max_results=5)

    print(f"Retrieved records: {len(works)}")

    for index, work in enumerate(works, start=1):
        title = work.get("title") or work.get("display_name") or "No title"
        year = work.get("publication_year") or "No year"
        print(f"{index}. {title} ({year})")