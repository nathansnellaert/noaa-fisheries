"""NOAA AFSC groundfish survey species — reference data for species identification."""
import pyarrow as pa
from connector_utils import fetch_paginated
from subsets_utils import save_raw_json, load_raw_json, load_state, save_state, overwrite, publish, data_hash

DATASET_ID = "noaa_groundfish_survey_species"

METADATA = {
    "id": DATASET_ID,
    "title": "NOAA Groundfish Survey Species",
    "description": "Species reference table for NOAA AFSC groundfish survey data. Maps species codes to scientific and common names with taxonomic database cross-references.",
    "license": "US Government Public Domain",
    "column_descriptions": {
        "species_code": "Unique species code used across groundfish survey tables",
        "scientific_name": "Scientific (Latin) name",
        "common_name": "Common name",
        "id_rank": "Taxonomic identification rank",
        "worms_id": "World Register of Marine Species (WoRMS) identifier",
        "itis_tsn": "Integrated Taxonomic Information System (ITIS) serial number",
    },
}


def download():
    state = load_state("groundfish_species")
    if state.get("completed"):
        print("  Species data already fetched")
        return

    print("  Fetching groundfish survey species...")
    items = fetch_paginated("afsc_groundfish_survey_species")
    save_raw_json(items, "groundfish_survey/species")
    print(f"  -> {len(items)} species records")

    save_state("groundfish_species", {"completed": True})


def transform():
    state = load_state("groundfish_species")
    if not state.get("completed"):
        print("  No species data to transform")
        return

    raw = load_raw_json("groundfish_survey/species")
    print(f"  Loaded {len(raw):,} species records")

    rows = [{
        "species_code": r["species_code"],
        "scientific_name": r.get("scientific_name"),
        "common_name": r.get("common_name"),
        "id_rank": r.get("id_rank"),
        "worms_id": r.get("worms"),
        "itis_tsn": r.get("itis"),
    } for r in raw]

    table = pa.Table.from_pylist(rows)

    h = data_hash(table)
    if load_state(DATASET_ID).get("hash") == h:
        print(f"  Skipping {DATASET_ID} - unchanged")
        return

    overwrite(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    save_state(DATASET_ID, {"hash": h})
    print(f"  Published {len(table):,} species records")


NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    from subsets_utils import validate_environment
    validate_environment()
    download()
    transform()
