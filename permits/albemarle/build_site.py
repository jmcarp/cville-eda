#!/usr/bin/env python
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "fiona",
#     "pydantic",
#     "pyproj",
#     "pyyaml",
#     "shapely",
# ]
# ///
"""Generate site/data.json and site/parcels.geojson from Albemarle plan data."""

import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from build_parcels import build_parcels
from build_projects import apply_overrides, find_projects, load_overrides
from models import AlbemarlePlan

BASE_PATH = Path(__file__).parent
PLANS_JSONL = BASE_PATH / "plans.jsonl"
OVERRIDES_YAML = BASE_PATH / "overrides.yaml"
PARCELS_ZIP = BASE_PATH / "parcels.zip"
HISTORICAL_DIR = BASE_PATH / "parcels_historical"
OUTPUT_PATH = BASE_PATH / "site" / "data.json"
GEOJSON_PATH = BASE_PATH / "site" / "parcels.geojson"


def load_plans(path: Path) -> dict[str, AlbemarlePlan]:
    """Load plans from JSONL file."""
    plans: dict[str, AlbemarlePlan] = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            plan = AlbemarlePlan.model_validate_json(line)
            plans[plan.plan_id] = plan
    return plans


def main() -> int:
    if not PLANS_JSONL.exists():
        print(f"Error: {PLANS_JSONL} not found. Run fetch_plans.py first.")
        return 1

    print("Loading plans...")
    plans = load_plans(PLANS_JSONL)
    print(f"Loaded {len(plans)} plans")

    print("Grouping into projects...")
    projects = find_projects(plans)
    print(f"Found {len(projects)} projects")

    if OVERRIDES_YAML.exists():
        print("Applying overrides...")
        overrides = load_overrides(OVERRIDES_YAML)
        projects = apply_overrides(projects, overrides)
        print(f"{len(projects)} projects after overrides")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    output_data = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "projects": projects,
    }

    with open(OUTPUT_PATH, "w") as f:
        json.dump(output_data, f, indent=2)

    print(f"Wrote {len(projects)} projects to {OUTPUT_PATH}")

    # Summary stats
    with_units = [p for p in projects if p.get("units")]
    total_units = sum(p["units"] for p in with_units)
    print(f"  {len(with_units)} projects with units ({total_units} total units)")

    # Build parcel GeoJSON if shapefile is available
    if PARCELS_ZIP.exists():
        print("Building parcel GeoJSON...")
        pin_to_projects: dict[str, list[dict]] = defaultdict(list)
        for project in projects:
            for pin in project.get("parcels", []):
                pin_to_projects[pin].append(project)

        # Historical parcel zips as fallback, newest-first
        fallback_zips = sorted(
            HISTORICAL_DIR.glob("Parcels*.zip"), reverse=True
        ) if HISTORICAL_DIR.exists() else []

        geojson = build_parcels(PARCELS_ZIP, dict(pin_to_projects), fallback_zips)
        with open(GEOJSON_PATH, "w") as f:
            json.dump(geojson, f)
        size_kb = GEOJSON_PATH.stat().st_size / 1024
        print(f"Wrote {GEOJSON_PATH} ({size_kb:.0f} KB)")
    else:
        print(f"Warning: {PARCELS_ZIP} not found, skipping parcels.geojson")

    return 0


if __name__ == "__main__":
    exit(main())
