"""NOAA FOSS international seafood trade data (1989-present)."""
from datetime import datetime
import pyarrow as pa
from connector_utils import fetch_paginated
from subsets_utils import save_raw_json, load_raw_json, load_state, save_state, overwrite, publish, data_hash

DATASET_ID = "noaa_fisheries_trade"

METADATA = {
    "id": DATASET_ID,
    "title": "NOAA International Seafood Trade",
    "description": "US international seafood trade data (1989-present). Covers imports and exports of fishery products by product, country, and customs district with weight and value.",
    "license": "US Government Public Domain",
    "column_descriptions": {
        "year": "Trade year",
        "month": "Trade month (01-12)",
        "hts_number": "Harmonized Tariff Schedule product code",
        "product_name": "Product name",
        "product_group_code": "FUS product group code",
        "product_subgroup_code": "FUS product subgroup code",
        "product_group": "Product group description",
        "product_subgroup": "Product subgroup description",
        "country_code": "Trading partner country code",
        "country": "Trading partner country name",
        "continent": "Continent of trading partner",
        "fao_code": "FAO country code",
        "customs_district_code": "US customs district code",
        "customs_district": "US customs district name",
        "edible_code": "Edible classification (E = edible, N = non-edible)",
        "kilos": "Weight in kilograms",
        "value_usd": "Trade value in US dollars",
        "source": "Trade direction (IMP = import, EXP = export, RE-EXP = re-export)",
        "association": "Trade association memberships (e.g. APEC, NAFTA)",
        "rfmo": "Regional Fisheries Management Organization",
        "nmfs_region_code": "NMFS region code",
    },
}


def download():
    state = load_state("trade")
    completed_years = set(state.get("completed_years", []))

    current_year = datetime.now().year
    pending_years = [y for y in range(1989, current_year + 1) if y not in completed_years]

    if not pending_years:
        print("  All trade years already fetched")
        return

    print(f"  Fetching {len(pending_years)} trade years...")
    for i, year in enumerate(pending_years, 1):
        print(f"  [{i}/{len(pending_years)}] Fetching trade {year}...")
        items = fetch_paginated("trade_data", {"year": str(year)})
        save_raw_json(items, f"trade/{year}")
        print(f"    -> {len(items)} records")

        completed_years.add(year)
        save_state("trade", {"completed_years": sorted(completed_years)})


def transform():
    state = load_state("trade")
    completed_years = state.get("completed_years", [])
    if not completed_years:
        print("  No trade data to transform")
        return

    tables = []
    for year in sorted(completed_years):
        raw = load_raw_json(f"trade/{year}")
        rows = [{
            "year": r.get("year"),
            "month": r.get("month"),
            "hts_number": r.get("hts_number"),
            "product_name": r.get("name"),
            "product_group_code": r.get("fus_group_code1"),
            "product_subgroup_code": r.get("fus_group_code2"),
            "product_group": r.get("fus_group1"),
            "product_subgroup": r.get("fus_group2"),
            "country_code": r.get("cntry_code"),
            "country": r.get("cntry_name"),
            "continent": r.get("continent"),
            "fao_code": r.get("fao"),
            "customs_district_code": r.get("custom_district_code"),
            "customs_district": r.get("custom_district_name"),
            "edible_code": r.get("edible_code"),
            "kilos": r.get("kilos"),
            "value_usd": r.get("val"),
            "source": r.get("source"),
            "association": r.get("association"),
            "rfmo": r.get("rfmo"),
            "nmfs_region_code": r.get("nmfs_region_code"),
        } for r in raw]
        if rows:
            tables.append(pa.Table.from_pylist(rows))

    table = pa.concat_tables(tables)
    print(f"  Loaded {len(table):,} trade records")

    h = data_hash(table)
    if load_state(DATASET_ID).get("hash") == h:
        print(f"  Skipping {DATASET_ID} - unchanged")
        return

    overwrite(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    save_state(DATASET_ID, {"hash": h})
    print(f"  Published {len(table):,} trade records")


NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    from subsets_utils import validate_environment
    validate_environment()
    download()
    transform()
