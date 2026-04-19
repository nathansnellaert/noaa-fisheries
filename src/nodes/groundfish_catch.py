"""NOAA AFSC groundfish survey catch — species counts and weights per haul."""
import pyarrow as pa
from connector_utils import fetch_paginated
from subsets_utils import save_raw_json, load_raw_json, load_state, save_state, overwrite, publish, data_hash

DATASET_ID = "noaa_groundfish_survey_catch"

METADATA = {
    "id": DATASET_ID,
    "title": "NOAA Groundfish Survey Catch",
    "description": "Species-level catch data from NOAA AFSC groundfish bottom trawl surveys. Each record represents one species caught in one haul, with catch-per-unit-effort metrics. Links to hauls table via hauljoin and species table via species_code.",
    "license": "US Government Public Domain",
    "column_descriptions": {
        "hauljoin": "Foreign key to groundfish survey hauls table",
        "species_code": "Species code (foreign key to species table)",
        "cpue_kg_km2": "Catch per unit effort in kg per square kilometer",
        "cpue_count_km2": "Catch per unit effort in count per square kilometer",
        "count": "Number of individuals caught",
        "weight_kg": "Total weight caught in kilograms",
        "taxon_confidence": "Taxonomic identification confidence rating",
    },
}


def download():
    state = load_state("groundfish_catch")
    if state.get("completed"):
        print("  Catch data already fetched")
        return

    print("  Fetching groundfish survey catch (all records)...")
    items = fetch_paginated("afsc_groundfish_survey_catch")
    save_raw_json(items, "groundfish_survey/catch")
    print(f"  -> {len(items)} catch records")

    save_state("groundfish_catch", {"completed": True})


def transform():
    state = load_state("groundfish_catch")
    if not state.get("completed"):
        print("  No catch data to transform")
        return

    raw = load_raw_json("groundfish_survey/catch")
    print(f"  Loaded {len(raw):,} catch records")

    rows = [{
        "hauljoin": r["hauljoin"],
        "species_code": r["species_code"],
        "cpue_kg_km2": r.get("cpue_kgkm2"),
        "cpue_count_km2": r.get("cpue_nokm2"),
        "count": r.get("count"),
        "weight_kg": r.get("weight_kg"),
        "taxon_confidence": r.get("taxon_confidence"),
    } for r in raw]

    table = pa.Table.from_pylist(rows)

    h = data_hash(table)
    if load_state(DATASET_ID).get("hash") == h:
        print(f"  Skipping {DATASET_ID} - unchanged")
        return

    overwrite(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    save_state(DATASET_ID, {"hash": h})
    print(f"  Published {len(table):,} catch records")


NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    from subsets_utils import validate_environment
    validate_environment()
    download()
    transform()
