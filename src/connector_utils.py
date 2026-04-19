"""NOAA FOSS API utilities."""
import json
from subsets_utils import get

BASE_URL = "https://apps-st.fisheries.noaa.gov/ods/foss"


def fetch_paginated(endpoint: str, query: dict = None) -> list[dict]:
    """Fetch all records from a NOAA FOSS API endpoint with pagination.

    Returns list of dicts with API 'links' field stripped.
    """
    all_items = []
    offset = 0
    limit = 10000
    page = 1

    while True:
        if query:
            url = f"{BASE_URL}/{endpoint}?q={json.dumps(query)}&limit={limit}&offset={offset}"
        else:
            url = f"{BASE_URL}/{endpoint}?limit={limit}&offset={offset}"

        response = get(url)
        data = response.json()

        items = data.get("items", [])
        for item in items:
            item.pop("links", None)
        all_items.extend(items)

        if page > 1:
            print(f"      page {page}: {len(items)} records (total: {len(all_items)})")

        if not data.get("hasMore", False):
            break

        offset += limit
        page += 1

    return all_items
