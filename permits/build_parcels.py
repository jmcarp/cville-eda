"""Build GeoJSON of parcels joined to project data for Charlottesville.

Reads raw GeoJSON from fetch_parcels.py (layer 72 — already in EPSG:4326),
filters to parcels with matching project PINs, simplifies geometry, and
attaches project metadata to each feature.
"""

import json
from pathlib import Path
from typing import Any

import shapely.geometry

# Higher priority = used for polygon color when multiple projects share a parcel.
# Cville statuses are uppercase.
_STATUS_PRIORITY = {
    "APPROVED": 100,
    "APPROVEDCL": 100,
    "UNDERCONST": 90,
    "PLANCOMM": 80,
    "REVIEW": 70,
    "RESUBMIT": 60,
    "COMMENTS": 50,
    "APPLIED": 40,
    "DEFERRED": 30,
    "EXPIRED": 20,
    "REJECTED": 10,
    "WITHDRAWN": 10,
    "DENIED": 10,
    "CLOSED": 10,
    "VOID": 0,
}


def _make_feature(
    pin: str,
    geom: shapely.geometry.base.BaseGeometry,
    projects: list[dict[str, Any]],
) -> dict:
    """Simplify geometry and wrap a parcel into a GeoJSON feature.

    The input geometry is already in EPSG:4326 (no reprojection needed).
    """
    # ~5m simplification to reduce file size (same tolerance as Albemarle)
    geom = geom.simplify(0.00005, preserve_topology=True)

    project_list = [
        {
            "plan_id": p["permit_id"],
            "project_name": (p.get("addresses") or [""])[0],
            "addresses": p.get("addresses", []),
            "units": p.get("units"),
            "status": p.get("status", ""),
            "plan_type": p.get("use_type", ""),
            "application_date": (
                p["initial_submit"].strftime("%Y-%m-%d")
                if p.get("initial_submit")
                else None
            ),
        }
        for p in projects
    ]

    best_status = max(
        (p["status"] for p in project_list),
        key=lambda s: _STATUS_PRIORITY.get(s, 0),
    )

    return {
        "type": "Feature",
        "geometry": shapely.geometry.mapping(geom),
        "properties": {
            "pin": pin,
            "status": best_status,
            "projects": project_list,
        },
    }


def build_parcels(
    geojson_path: Path,
    pin_to_projects: dict[str, list[dict[str, Any]]],
) -> dict:
    """Read raw GeoJSON, filter to matched PINs, simplify, and annotate.

    Args:
        geojson_path: Path to raw parcels_geo.geojson from fetch_parcels.py
        pin_to_projects: Mapping of ParcelNumber -> list of project dicts

    Returns:
        GeoJSON FeatureCollection dict
    """
    with open(geojson_path) as f:
        raw = json.load(f)

    features: list[dict] = []
    matched_pins: set[str] = set()

    for feature in raw.get("features", []):
        props = feature.get("properties", {})
        pin = props.get("ParcelNumber", "")
        if pin not in pin_to_projects:
            continue

        geom = shapely.geometry.shape(feature["geometry"])
        features.append(_make_feature(pin, geom, pin_to_projects[pin]))
        matched_pins.add(pin)

    unmatched = set(pin_to_projects.keys()) - matched_pins
    multi = sum(1 for f in features if len(f["properties"]["projects"]) > 1)
    print(f"  Matched {len(matched_pins)} PINs, {len(unmatched)} unmatched")
    print(f"  {len(features)} features ({multi} with multiple projects)")

    return {"type": "FeatureCollection", "features": features}
