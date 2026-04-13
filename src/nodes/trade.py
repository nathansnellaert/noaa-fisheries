"""Ingest NOAA FOSS international seafood trade data.

This node downloads international seafood trade data from the NOAA FOSS API.
Data is fetched year by year from 1989 to present.
"""
from datetime import datetime
from subsets_utils import get, save_raw_json, load_state, save_state

BASE_URL = "https://apps-st.fisheries.noaa.gov/ods/foss/trade_data"


def run():
    """Fetch international seafood trade data from NOAA FOSS API."""
    print("Processing trade data...")

    state = load_state("trade")
    completed_years = set(state.get("completed_years", []))

    # Trade data starts from 1989
    current_year = datetime.now().year
    years = list(range(1989, current_year + 1))
    pending_years = [y for y in years if y not in completed_years]

    if not pending_years:
        print("  All years already fetched")
        return

    print(f"  Fetching {len(pending_years)} years...")

    for i, year in enumerate(pending_years, 1):
        print(f"  [{i}/{len(pending_years)}] Fetching {year}...")

        # API has max 10000 records per request, need to paginate
        all_items = []
        offset = 0
        limit = 10000
        page = 1

        while True:
            response = get(f"{BASE_URL}?q={{\"year\":\"{year}\"}}&limit={limit}&offset={offset}")
            data = response.json()

            items = data.get("items", [])
            all_items.extend(items)

            if page > 1:
                print(f"    page {page}: {len(items)} records (total: {len(all_items)})")

            if not data.get("hasMore", False):
                break

            offset += limit
            page += 1

        save_raw_json(all_items, f"trade/{year}")
        print(f"    -> {len(all_items)} records")

        completed_years.add(year)
        save_state("trade", {"completed_years": list(completed_years)})

    print("  Done!")


NODES = {
    run: [],
}


if __name__ == "__main__":
    from subsets_utils import validate_environment
    validate_environment()
    run()
