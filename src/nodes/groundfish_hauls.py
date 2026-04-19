"""NOAA AFSC groundfish survey hauls — survey metadata with location, conditions, and effort."""
from datetime import datetime
import pyarrow as pa
from connector_utils import fetch_paginated
from subsets_utils import save_raw_json, load_raw_json, load_state, save_state, overwrite, publish, data_hash

DATASET_ID = "noaa_groundfish_survey_hauls"

METADATA = {
    "id": DATASET_ID,
    "title": "NOAA Groundfish Survey Hauls",
    "description": "Survey haul metadata from NOAA AFSC groundfish bottom trawl surveys in Alaska (1982-present). Each record represents one trawl haul with location, environmental conditions, and sampling effort.",
    "license": "US Government Public Domain",
    "column_descriptions": {
        "year": "Survey year",
        "survey_code": "Short survey region code (EBS, GOA, AI, NBS, BSS)",
        "survey_region": "Survey region name",
        "survey_name": "Full survey name",
        "survey_definition_id": "Survey definition identifier",
        "cruise": "Cruise identifier",
        "cruisejoin": "Cruise join key for linking tables",
        "hauljoin": "Unique haul identifier (primary key)",
        "haul": "Haul number within cruise",
        "stratum": "Survey stratum",
        "station": "Station identifier",
        "vessel_id": "Vessel identifier",
        "vessel_name": "Vessel name",
        "date_time": "Date and time of haul (UTC)",
        "latitude_start": "Start latitude in decimal degrees",
        "longitude_start": "Start longitude in decimal degrees",
        "latitude_end": "End latitude in decimal degrees",
        "longitude_end": "End longitude in decimal degrees",
        "bottom_temperature_c": "Bottom temperature in degrees Celsius",
        "surface_temperature_c": "Surface temperature in degrees Celsius",
        "depth_m": "Depth in meters",
        "distance_fished_km": "Distance fished in kilometers",
        "duration_hr": "Haul duration in hours",
        "net_width_m": "Net width in meters",
        "net_height_m": "Net height in meters",
        "area_swept_km2": "Area swept in square kilometers",
        "performance": "Haul performance code (0 = satisfactory)",
    },
}


def download():
    state = load_state("groundfish_hauls")
    completed_years = set(state.get("completed_years", []))

    current_year = datetime.now().year
    pending_years = [y for y in range(1982, current_year + 1) if y not in completed_years]

    if not pending_years:
        print("  All haul years already fetched")
        return

    print(f"  Fetching {len(pending_years)} haul years...")
    for i, year in enumerate(pending_years, 1):
        print(f"  [{i}/{len(pending_years)}] Fetching hauls {year}...")
        items = fetch_paginated("afsc_groundfish_survey_haul", {"year": str(year)})
        save_raw_json(items, f"groundfish_survey/hauls/{year}")
        print(f"    -> {len(items)} records")

        completed_years.add(year)
        save_state("groundfish_hauls", {"completed_years": sorted(completed_years)})


def transform():
    state = load_state("groundfish_hauls")
    completed_years = state.get("completed_years", [])
    if not completed_years:
        print("  No haul data to transform")
        return

    rows = []
    for year in sorted(completed_years):
        for r in load_raw_json(f"groundfish_survey/hauls/{year}"):
            rows.append({
                "year": r["year"],
                "survey_code": r.get("srvy"),
                "survey_region": r.get("survey"),
                "survey_name": r.get("survey_name"),
                "survey_definition_id": r.get("survey_definition_id"),
                "cruise": r.get("cruise"),
                "cruisejoin": r.get("cruisejoin"),
                "hauljoin": r["hauljoin"],
                "haul": r.get("haul"),
                "stratum": r.get("stratum"),
                "station": r.get("station"),
                "vessel_id": r.get("vessel_id"),
                "vessel_name": r.get("vessel_name"),
                "date_time": r.get("date_time"),
                "latitude_start": r.get("latitude_dd_start"),
                "longitude_start": r.get("longitude_dd_start"),
                "latitude_end": r.get("latitude_dd_end"),
                "longitude_end": r.get("longitude_dd_end"),
                "bottom_temperature_c": r.get("bottom_temperature_c"),
                "surface_temperature_c": r.get("surface_temperature_c"),
                "depth_m": r.get("depth_m"),
                "distance_fished_km": r.get("distance_fished_km"),
                "duration_hr": r.get("duration_hr"),
                "net_width_m": r.get("net_width_m"),
                "net_height_m": r.get("net_height_m"),
                "area_swept_km2": r.get("area_swept_km2"),
                "performance": r.get("performance"),
            })

    print(f"  Loaded {len(rows)} haul records")
    table = pa.Table.from_pylist(rows)

    h = data_hash(table)
    if load_state(DATASET_ID).get("hash") == h:
        print(f"  Skipping {DATASET_ID} - unchanged")
        return

    overwrite(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    save_state(DATASET_ID, {"hash": h})
    print(f"  Published {len(table):,} haul records")


NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    from subsets_utils import validate_environment
    validate_environment()
    download()
    transform()
