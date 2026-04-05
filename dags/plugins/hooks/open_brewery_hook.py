"""
open_brewery_hook.py
=====================
Airflow Hook for the Open Brewery DB public API.

This module encapsulates all HTTP communication with the Open Brewery DB
API, including session management, page-based pagination, and exponential
back-off retry logic with jitter.

Keeping this logic in a Hook — rather than inline in an Operator — means
any future Operator (e.g. a Snowflake loader) can reuse the same extraction
logic without duplicating a single line of pagination or retry code.

Example:
    Typical usage inside an Airflow Operator::

        from plugins.hooks.open_brewery_hook import OpenBreweryHook

        hook = OpenBreweryHook(
            url="https://api.openbrewerydb.org/v1/breweries",
            max_retries=3,
            register_per_page=200,
            exponential_backoff_seconds=2,
        )
        records = hook.get_breweries()
"""

import logging
import random
import time
from typing import Any, Dict, List

import requests
from airflow.sdk.bases.hook.BaseHook import BaseHook

from plugins.utils.constants import API_TIMEOUT

logger = logging.getLogger(__name__)


class OpenBreweryHook(BaseHook):
    """Airflow Hook for paginated extraction from the Open Brewery DB API.

    Manages an HTTP ``requests.Session`` for connection reuse across pages
    (avoiding repeated TCP/TLS handshake overhead), and applies exponential
    back-off with random jitter on transient failures.

    Attributes:
        base_url (str): The API endpoint to paginate.
        max_registries_per_page (int): Records requested per page call.
        max_retries (int): Maximum attempts per page request.
        exponential_backoff_seconds (int): Base back-off interval in seconds.
        api_timeout (int): Per-request timeout in seconds.

    Args:
        url (str): Full URL of the breweries API endpoint.
        max_retries (int): Maximum number of retry attempts for each page.
        register_per_page (int): Number of records to request per page.
            The API maximum is 200.
        exponential_backoff_seconds (int): Base seconds for back-off
            calculation. Actual sleep = ``base * random.randint(1, 4)``.
        *args: Passed through to ``BaseHook.__init__``.
        **kwargs: Passed through to ``BaseHook.__init__``.
    """

    def __init__(
        self,
        url: str,
        max_retries: int,
        register_per_page: int,
        exponential_backoff_seconds: int,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.base_url = url
        self.max_registries_per_page = register_per_page
        self.max_retries = max_retries
        self.exponential_backoff_seconds = exponential_backoff_seconds
        self.api_timeout = API_TIMEOUT

    def get_breweries(self) -> List[Dict[str, Any]]:
        """Fetch every brewery record from the API via paginated requests.

        Iterates pages sequentially starting at ``page=1``. Two stop
        conditions are checked on each iteration (first one wins):

        1. The API returns an empty list — no more pages exist.
        2. The page contains fewer records than ``per_page`` — last page.

        A ``requests.Session`` is reused across all page requests so that
        the TCP connection and TLS handshake are only established once,
        regardless of how many pages are fetched.

        Returns:
            List[Dict[str, Any]]: Raw brewery records exactly as returned
            by the API. No transformation is applied here — that is the
            responsibility of the silver layer operator.

        Raises:
            requests.exceptions.HTTPError: On a 4xx client error (fast-fail,
                no retry).
            RuntimeError: If all retry attempts are exhausted for any page.
        """
        all_breweries: List[Dict] = []
        page = 1

        session = requests.Session()
        session.headers.update({"Accept": "application/json"})

        while True:
            params = {"page": page, "per_page": self.max_registries_per_page}

            breweries = self._get_breweries_with_retry_and_backoff(
                session=session,
                params=params,
                base_url=self.base_url,
            )

            if not breweries:
                logger.warning("No more breweries to fetch at page %d", page)
                break

            all_breweries.extend(breweries)
            logger.info("Fetched %d breweries from page %d", len(breweries), page)

            if len(breweries) < self.max_registries_per_page:
                logger.info(
                    "Last page reached at page %d with %d breweries",
                    page, len(breweries),
                )
                break

            page += 1

        logger.info("Successfully fetched %d breweries in total", len(all_breweries))
        return all_breweries

    def _get_breweries_with_retry_and_backoff(
        self,
        session: requests.Session,
        params: Dict[str, int],
        base_url: str,
    ) -> List[Dict[str, Any]]:
        """Execute a single GET request with retry and exponential back-off.

        Retry behaviour by exception type:

        - **4xx HTTPError**: Raised immediately — client errors will not
          be resolved by retrying.
        - **5xx HTTPError**: Logged and retried with back-off. Raises on
          the final attempt.
        - **Other RequestException** (timeout, connection error): Logged
          and retried with back-off. Raises on the final attempt.

        The back-off duration includes random jitter
        (``base * random.randint(1, 4)``) to prevent thundering-herd
        behaviour when multiple workers fail simultaneously.

        Args:
            session (requests.Session): Active session with shared headers.
            params (Dict[str, int]): Query parameters, typically
                ``{"page": N, "per_page": M}``.
            base_url (str): The endpoint URL to GET.

        Returns:
            List[Dict[str, Any]]: Parsed JSON response body.

        Raises:
            requests.exceptions.HTTPError: Immediately on 4xx; after all
                retries on 5xx.
            requests.exceptions.RequestException: After all retries on
                network-level failures.
        """
        for attempt in range(1, self.max_retries + 1):
            try:
                response = session.get(url=base_url, params=params, timeout=self.api_timeout)
                response.raise_for_status()
                return response.json()

            except requests.exceptions.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else "unknown"

                if exc.response is not None and 400 <= exc.response.status_code < 500:
                    logger.error("Client error %s — not retrying. params=%s", status, params)
                    raise

                # 5xx — log, back-off, retry
                logger.warning(
                    "Server error %s on attempt %d/%d — retrying in %ds",
                    status, attempt, self.max_retries,
                    self.exponential_backoff_seconds * attempt,
                )
                if attempt == self.max_retries:
                    raise
                time.sleep(self.exponential_backoff_seconds * random.randint(1, 4))

            except requests.exceptions.RequestException as err:
                logger.warning(
                    "Request error on attempt %d/%d: %s",
                    attempt, self.max_retries, err,
                )
                if attempt < self.max_retries:
                    backoff_time = self.exponential_backoff_seconds * random.randint(1, 4)
                    logger.info("Retrying after %ds", backoff_time)
                    time.sleep(backoff_time)
                else:
                    logger.error(
                        "Maximum retries (%d) reached. Last error: %s",
                        self.max_retries, err,
                    )
                    raise