"""NOAA FOSS commercial fisheries landings data (1950-present)."""
from datetime import datetime
import pyarrow as pa
from connector_utils import fetch_paginated
from subsets_utils import save_raw_json, load_raw_json, load_state, save_state, overwrite, publish, data_hash

DATASET_ID = "noaa_fisheries_landings"

METADATA = {
    "id": DATASET_ID,
    "title": "NOAA Commercial Fisheries Landings",
    "description": "Commercial and recreational fisheries landings in the United States (1950-present). Covers weight, value, and count of fish landed by species, state, and data source.",
    "license": "US Government Public Domain",
    "column_descriptions": {
        "year": "Year of landing",
        "tsn": "ITIS Taxonomic Serial Number",
        "species_name": "Common species name (AFS standard)",
        "scientific_name": "Scientific name",
        "region": "NOAA fisheries region",
        "state": "US state",
        "pounds": "Weight landed in pounds",
        "dollars": "Value in US dollars",
        "count": "Total count of fish",
        "source": "Data source (e.g. MRIP, state/federal agencies)",
        "collection": "Collection type (Commercial or Recreational)",
    },
}


def download():
    state = load_state("landings")
    completed_years = set(state.get("completed_years", []))

    current_year = datetime.now().year
    pending_years = [y for y in range(1950, current_year + 1) if y not in completed_years]

    if not pending_years:
        print("  All landings years already fetched")
        return

    print(f"  Fetching {len(pending_years)} landings years...")
    for i, year in enumerate(pending_years, 1):
        print(f"  [{i}/{len(pending_years)}] Fetching landings {year}...")
        items = fetch_paginated("landings", {"year": str(year)})
        save_raw_json(items, f"landings/{year}")
        print(f"    -> {len(items)} records")

        completed_years.add(year)
        save_state("landings", {"completed_years": sorted(completed_years)})


def transform():
    state = load_state("landings")
    completed_years = state.get("completed_years", [])
    if not completed_years:
        print("  No landings data to transform")
        return

    tables = []
    for year in sorted(completed_years):
        raw = load_raw_json(f"landings/{year}")
        rows = [{
            "year": r["year"],
            "tsn": r.get("tsn"),
            "species_name": r.get("ts_afs_name"),
            "scientific_name": r.get("ts_scientific_name"),
            "region": r.get("region_name"),
            "state": r.get("state_name"),
            "pounds": r.get("pounds"),
            "dollars": r.get("dollars"),
            "count": r.get("tot_count"),
            "source": r.get("source"),
            "collection": r.get("collection"),
        } for r in raw]
        if rows:
            tables.append(pa.Table.from_pylist(rows))

    table = pa.concat_tables(tables)
    print(f"  Loaded {len(table):,} landings records")

    h = data_hash(table)
    if load_state(DATASET_ID).get("hash") == h:
        print(f"  Skipping {DATASET_ID} - unchanged")
        return

    overwrite(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    save_state(DATASET_ID, {"hash": h})
    print(f"  Published {len(table):,} landings records")


NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    from subsets_utils import validate_environment
    validate_environment()
    download()
    transform()
