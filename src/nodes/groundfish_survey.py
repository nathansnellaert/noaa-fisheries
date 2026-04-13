"""Ingest NOAA AFSC groundfish survey data.

This node downloads groundfish survey data from the NOAA FOSS API, including:
- Hauls (survey metadata with location, conditions, effort)
- Catch (species counts and weights per haul)
- Species (reference data for species identification)
"""
from datetime import datetime
from subsets_utils import get, save_raw_json, load_state, save_state

BASE_URL = "https://apps-st.fisheries.noaa.gov/ods/foss"


def _fetch_hauls():
    """Fetch survey haul metadata (location, conditions, effort)."""
    print("  Fetching groundfish survey hauls...")

    state = load_state("groundfish_hauls")
    completed_years = set(state.get("completed_years", []))

    # Survey data starts from 1982
    current_year = datetime.now().year
    years = list(range(1982, current_year + 1))
    pending_years = [y for y in years if y not in completed_years]

    if not pending_years:
        print("    All years already fetched")
        return

    print(f"    Fetching {len(pending_years)} years...")

    for i, year in enumerate(pending_years, 1):
        print(f"    [{i}/{len(pending_years)}] Fetching hauls {year}...")

        # API has max 10000 records per request, need to paginate
        all_items = []
        offset = 0
        limit = 10000
        page = 1

        while True:
            response = get(
                f"{BASE_URL}/afsc_groundfish_survey_haul?q={{\"year\":\"{year}\"}}&limit={limit}&offset={offset}"
            )
            data = response.json()

            items = data.get("items", [])
            all_items.extend(items)

            if page > 1:
                print(f"      page {page}: {len(items)} records (total: {len(all_items)})")

            if not data.get("hasMore", False):
                break

            offset += limit
            page += 1

        save_raw_json(all_items, f"groundfish_survey/hauls/{year}")
        print(f"      -> {len(all_items)} records")

        completed_years.add(year)
        save_state("groundfish_hauls", {"completed_years": list(completed_years)})


def _fetch_catch():
    """Fetch survey catch data (species counts and weights per haul).

    Note: Catch data doesn't have year field - it links to hauls via hauljoin.
    We fetch all records using pagination since the dataset is large.
    """
    print("  Fetching groundfish survey catch...")

    state = load_state("groundfish_catch")
    if state.get("completed"):
        print("    Already fetched")
        return

    all_items = []
    offset = 0
    limit = 10000  # API max is 10000 per request
    page = 1

    while True:
        print(f"    Fetching catch page {page} (offset {offset})...")

        response = get(f"{BASE_URL}/afsc_groundfish_survey_catch?limit={limit}&offset={offset}")
        data = response.json()

        items = data.get("items", [])
        all_items.extend(items)
        print(f"      -> {len(items)} records (total: {len(all_items)})")

        if not data.get("hasMore", False):
            break

        offset += limit
        page += 1

    save_raw_json(all_items, "groundfish_survey/catch")
    print(f"    Total catch records: {len(all_items)}")

    save_state("groundfish_catch", {"completed": True})


def _fetch_species():
    """Fetch species reference data (one-time, not year-based)."""
    print("  Fetching groundfish survey species...")

    state = load_state("groundfish_species")
    if state.get("completed"):
        print("    Already fetched")
        return

    # Fetch all species with pagination (API max 10000 per request)
    all_items = []
    offset = 0
    limit = 10000
    page = 1

    while True:
        response = get(f"{BASE_URL}/afsc_groundfish_survey_species?limit={limit}&offset={offset}")
        data = response.json()

        items = data.get("items", [])
        all_items.extend(items)

        if page > 1:
            print(f"      page {page}: {len(items)} records (total: {len(all_items)})")

        if not data.get("hasMore", False):
            break

        offset += limit
        page += 1

    save_raw_json(all_items, "groundfish_survey/species")
    print(f"    -> {len(all_items)} records")

    save_state("groundfish_species", {"completed": True})


def run():
    """Fetch AFSC groundfish survey data from NOAA FOSS API."""
    print("Processing groundfish survey data...")
    _fetch_hauls()
    _fetch_catch()
    _fetch_species()
    print("  Done!")


NODES = {
    run: [],
}


if __name__ == "__main__":
    from subsets_utils import validate_environment
    validate_environment()
    run()
