from typing import Dict, List

MEDALION_ARCHITECTURE_LAYERS = ["bronze-layer", "silver-layer", "gold-layer"]

# API
BREWERIES_API_URL = "https://api.openbrewerydb.org/v1/breweries"
MAX_RETRIES = 3
MAX_REGISTRIES_PER_PAGE = 200
EXPONENTIAL_BACKOFF_SECONDS = 2
API_TIMEOUT = 10

# Silver

SILVER_SCHEMA_MAPPING: Dict =    {
    "id": "string",
    "name": "string",
    "brewery_type": "category",
    "address_1": "string",
    "address_2": "string",
    "address_3": "string",
    "city": "string",
    "state_province": "string",
    "postal_code": "string",
    "country": "string",
    "longitude": "float64",
    "latitude": "float64",
    "phone": "string",
    "website_url": "string",
    "state": "string",
    "street": "string"
}

NOT_NULL_COLUMNS: List[str] = [
    "id",
    "name",
    "brewery_type",
    "country",
]

# Known valid brewery types from the API spec
VALID_BREWERY_TYPES: set[str] = {
    "micro", "nano", "regional", "brewpub", "large",
    "planning", "bar", "contract", "proprietor", "closed",
}
