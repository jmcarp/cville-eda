"""Build GeoJSON of parcels joined to project data."""

from pathlib import Path
from typing import Any

import fiona
import pyproj
import shapely.geometry
import shapely.ops

# Higher priority = used for polygon color when multiple projects share a parcel
_STATUS_PRIORITY = {
    "In Review": 80,
    "Submitted": 75,
    "Submitted - Online": 75,
    "Fees Paid": 65,
    "Fees Due": 60,
    "On Hold": 40,
    "Deferred Definite": 30,
    "Deferred Indefinite": 30,
    "Approved": 100,
    "Complete": 70,
}


def build_parcels(
    zip_path: Path, pin_to_projects: dict[str, list[dict[str, Any]]]
) -> dict:
    """Read parcels from shapefile zip, reproject, filter to matched PINs.

    Emits one feature per parcel with a `projects` array, and a top-level
    `status` for polygon coloring (highest-priority status among projects).

    Args:
        zip_path: Path to parcels_shape_current.zip
        pin_to_projects: Mapping of PIN -> list of project dicts to attach

    Returns:
        GeoJSON FeatureCollection dict
    """
    transformer = pyproj.Transformer.from_crs(
        "EPSG:2284", "EPSG:4326", always_xy=True
    )

    features: list[dict] = []
    matched_pins: set[str] = set()

    shp_path = f"zip://{zip_path}!Parcels_current.shp"

    with fiona.open(shp_path) as src:
        for feature in src:
            pin = feature["properties"].get("PIN", "")
            if pin not in pin_to_projects:
                continue

            geom = shapely.geometry.shape(feature["geometry"])
            geom = shapely.ops.transform(transformer.transform, geom)
            # ~5m simplification to reduce file size
            geom = geom.simplify(0.00005, preserve_topology=True)

            matched_pins.add(pin)

            projects = pin_to_projects[pin]
            project_list = [
                {
                    "plan_id": p["plan_id"],
                    "project_name": p.get("project_name") or "",
                    "addresses": p.get("addresses", []),
                    "units": p.get("units"),
                    "status": p.get("status", ""),
                    "plan_type": p.get("plan_type", ""),
                    "application_date": p.get("application_date"),
                }
                for p in projects
            ]

            # Pick the highest-priority status for polygon coloring
            best_status = max(
                (p["status"] for p in project_list),
                key=lambda s: _STATUS_PRIORITY.get(s, 0),
            )

            features.append(
                {
                    "type": "Feature",
                    "geometry": shapely.geometry.mapping(geom),
                    "properties": {
                        "pin": pin,
                        "status": best_status,
                        "projects": project_list,
                    },
                }
            )

    unmatched = set(pin_to_projects.keys()) - matched_pins
    multi = sum(1 for f in features if len(f["properties"]["projects"]) > 1)
    print(f"  Matched {len(matched_pins)} PINs, {len(unmatched)} unmatched")
    print(f"  {len(features)} features ({multi} with multiple projects)")

    return {"type": "FeatureCollection", "features": features}
