from airflow.hooks.base import BaseHook
from plugins.utils.constants import (
    API_TIMEOUT
)

import requests
from typing import List, Dict, Any
import time
import random

import logging
logger = logging.getLogger(__name__)  

class OpenBreweryHook(BaseHook):
    def __init__(
            self, 
            url : str, 
            max_retries : int, 
            register_per_page : int, 
            exponential_backoff_seconds : int, 
            *args,
            **kwargs
        ):
        super().__init__(*args, **kwargs)

        self.base_url = url
        self.max_registries_per_page = register_per_page
        self.max_retries = max_retries
        self.exponential_backoff_seconds = exponential_backoff_seconds
        self.api_timeout = API_TIMEOUT

    def get_breweries(self) -> List[Dict[str, Any]]:
        # Fetches Every brewery

        all_breweries : List[Dict] = []
        page = 1

        session = requests.Session()
        session.headers.update({"Accept": "application/json"})

        while True:
            params = {"page": page, "per_page": self.max_registries_per_page}

            breweries = self._get_breweries_with_retry_and_backoff(
                session=session,
                params=params,
                base_url=self.base_url
            )

            if not breweries:
                logger.warning(f"No more breweries to fetch at page {page}")
                break

            all_breweries.extend(breweries)

            logger.info(f"Fetched {len(breweries)} breweries from page {page}")

            if len(breweries) < self.max_registries_per_page:
                logger.info(f"Last page reached at page {page} with {len(breweries)} breweries")
                break

            page += 1

        logger.info(f"Successfully fetched {len(all_breweries)} breweries in total from the API")

        return all_breweries
    
    def _get_breweries_with_retry_and_backoff(
        self,
        session: requests.Session,
        params: Dict[str, int],
        base_url: str
    ) -> List[Dict[str, Any]]:
        
        for attempt in range(1, self.max_retries + 1):
            try:
                response = session.get(url=base_url, params=params, timeout=self.api_timeout)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as exc:
                if exc.response is not None and 400 <= exc.response.status_code < 500:
                    raise  # fast-fail, don't retry client errors
                # else fall through to retry logic
            except requests.exceptions.RequestException as err:
                logger.warning(f"Error fetching breweries: {err}")

                if attempt < self.max_retries:
                    backoff_time = self.exponential_backoff_seconds * random.randint(1, 4)
                    logger.info(f"Retrying after {backoff_time} seconds (attempt {attempt}/{self.max_retries})")
                    time.sleep(backoff_time)
                else:
                    logger.error(f"Maximum retries ({self.max_retries}) reached for fetching breweries. Last error: {err}")
                    raise
            