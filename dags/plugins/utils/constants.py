"""
constants.py
============
Centralized configuration constants for the Bees Challenge pipeline.

This module holds all shared configuration values consumed by hooks,
operators, and utilities across the medallion architecture layers
(bronze, silver, gold). Keeping them here avoids magic numbers scattered
across files and makes environment-specific tuning straightforward.

Note:
    Any value that may differ between environments (dev/staging/prod)
    should be migrated to Airflow Variables or environment variables
    and referenced here via ``os.getenv`` or ``Variable.get``.
"""

from typing import Dict, List, Set

# ---------------------------------------------------------------------------
# Medallion Architecture
# ---------------------------------------------------------------------------

MEDALION_ARCHITECTURE_LAYERS: List[str] = [
    "bronze-layer",
    "silver-layer",
    "gold-layer",
]
"""Ordered list of S3 bucket names for each medallion layer."""

# ---------------------------------------------------------------------------
# Open Brewery DB API
# ---------------------------------------------------------------------------

BREWERIES_API_URL: str = "https://api.openbrewerydb.org/v1/breweries"
"""Base endpoint for the Open Brewery DB breweries resource."""

MAX_RETRIES: int = 3
"""Maximum number of HTTP request attempts before raising an exception."""

MAX_REGISTRIES_PER_PAGE: int = 200
"""Records requested per paginated API call. 200 is the API maximum."""

EXPONENTIAL_BACKOFF_SECONDS: int = 2
"""
Base number of seconds used in the exponential back-off formula.

The actual sleep time per attempt is:
    ``EXPONENTIAL_BACKOFF_SECONDS * random.randint(1, 4)``

This introduces jitter to avoid thundering-herd problems when multiple
workers retry simultaneously.
"""

API_TIMEOUT: int = 10
"""Seconds before an outbound HTTP request is considered timed out."""

# ---------------------------------------------------------------------------
# Silver Layer — Schema Contract
# ---------------------------------------------------------------------------

SILVER_SCHEMA_MAPPING: Dict[str, str] = {
    "id":             "string",
    "name":           "string",
    "brewery_type":   "category",
    "address_1":      "string",
    "address_2":      "string",
    "address_3":      "string",
    "city":           "string",
    "state_province": "string",
    "postal_code":    "string",
    "country":        "string",
    "longitude":      "float64",
    "latitude":       "float64",
    "phone":          "string",
    "website_url":    "string",
    "state":          "string",
    "street":         "string",
}
"""
Column-to-dtype mapping defining the silver layer schema contract.

``brewery_type`` is cast to ``category`` to reduce memory footprint and
enable ``observed=True`` in ``groupby`` calls, which prevents empty
partition files for unseen category combinations.

``longitude`` and ``latitude`` use ``pd.to_numeric(..., errors='coerce')``
so malformed coordinate strings become ``NaN`` rather than raising.
"""

NOT_NULL_COLUMNS: List[str] = [
    "id",
    "name",
    "brewery_type",
    "country",
]
"""
Columns that must never be null in the silver layer.

Rows violating this constraint are dropped in ``_handle_nulls`` with a
warning log. These columns represent the minimum identity fields required
for a brewery record to be analytically meaningful.
"""

VALID_BREWERY_TYPES: Set[str] = {
    "micro", "nano", "regional", "brewpub", "large",
    "planning", "bar", "contract", "proprietor", "closed",
}
"""
Set of brewery type values recognised by the Open Brewery DB API spec.

Values outside this set encountered in production data (e.g. ``taproom``,
``cidery``, ``beergarden``) are converted to ``NaN`` when the column is
cast to ``pd.Categorical`` with ``categories=VALID_BREWERY_TYPES``.
"""