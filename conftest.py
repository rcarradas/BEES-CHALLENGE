"""
conftest.py
============
Shared pytest fixtures for the Bees Challenge test suite.

All fixtures here are available to every test file without import.
Fixtures that require Airflow context mock it minimally — only the
fields actually accessed by the operators under test.
"""

import io
import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Airflow context mock
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_airflow_context():
    """Minimal Airflow task context used by operators in execute()."""

    class FakeDag:
        dag_id = "test_dag"

    return {
        "dag":          FakeDag(),
        "run_id":       "test_run_id_001",
        "logical_date": "2024-01-01T00:00:00+00:00",
    }


# ---------------------------------------------------------------------------
# Sample brewery records (mirrors real API shape)
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_brewery_records():
    """10 realistic brewery records as returned by the Open Brewery DB API."""
    return [
        {
            "id": f"brewery-{i}",
            "name": f"Test Brewery {i}",
            "brewery_type": btype,
            "address_1": f"{i} Main St",
            "address_2": None,
            "address_3": None,
            "city": city,
            "state_province": state,
            "postal_code": "12345",
            "country": country,
            "longitude": str(-120.0 + i),
            "latitude": str(37.0 + i),
            "phone": "5551234567",
            "website_url": f"https://brewery{i}.com",
            "state": state,
            "street": f"{i} Main St",
        }
        for i, (btype, city, state, country) in enumerate([
            ("micro",      "San Francisco", "California",    "United States"),
            ("brewpub",    "Portland",      "Oregon",        "United States"),
            ("micro",      "Austin",        "Texas",         "United States"),
            ("nano",       "Denver",        "Colorado",      "United States"),
            ("regional",   "Chicago",       "Illinois",      "United States"),
            ("micro",      "Vienna",        "Vienna",        "Austria"),
            ("brewpub",    "Munich",        "Bavaria",       "Germany"),
            ("large",      "London",        "England",       "United Kingdom"),
            ("planning",   "Toronto",       "Ontario",       "Canada"),
            ("closed",     "Sydney",        "New South Wales", "Australia"),
        ])
    ]


@pytest.fixture
def sample_dataframe(sample_brewery_records):
    """DataFrame built from sample_brewery_records with bronze audit columns."""
    df = pd.DataFrame(sample_brewery_records)
    df["_ingested_at"]  = "2024-01-01T03:00:00+00:00"
    df["_source"]       = "https://api.openbrewerydb.org/v1/breweries"
    df["_dag_id"]       = "test_dag"
    df["_dag_run_id"]   = "test_run_id_001"
    df["_logical_date"] = "2024-01-01T00:00:00+00:00"
    return df


@pytest.fixture
def sample_parquet_buffer(sample_dataframe):
    """In-memory Parquet buffer of sample_dataframe, ready for S3 mocking."""
    buf = io.BytesIO()
    sample_dataframe.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    return buf