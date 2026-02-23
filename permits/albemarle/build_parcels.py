"""Build GeoJSON of parcels joined to project data."""

import zipfile
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


def _find_shp_name(zip_path: Path) -> str:
    """Find the .shp filename inside a parcel zip archive."""
    with zipfile.ZipFile(zip_path) as zf:
        shp_files = [n for n in zf.namelist() if n.lower().endswith(".shp")]
        if not shp_files:
            raise ValueError(f"No .shp file found in {zip_path}")
        return shp_files[0]


def _make_feature(
    pin: str,
    geom: shapely.geometry.base.BaseGeometry,
    projects: list[dict[str, Any]],
    transformer: pyproj.Transformer,
) -> dict:
    """Reproject, simplify, and wrap a parcel geometry into a GeoJSON feature."""
    geom = shapely.ops.transform(transformer.transform, geom)
    # ~5m simplification to reduce file size
    geom = geom.simplify(0.00005, preserve_topology=True)

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


def _match_pins_from_zip(
    zip_path: Path,
    remaining_pins: dict[str, list[dict[str, Any]]],
    transformer: pyproj.Transformer,
) -> tuple[list[dict], set[str]]:
    """Scan a shapefile zip for PINs, return (features, matched_pin_set)."""
    shp_name = _find_shp_name(zip_path)
    shp_path = f"zip://{zip_path}!{shp_name}"

    features: list[dict] = []
    matched: set[str] = set()

    with fiona.open(shp_path) as src:
        for feature in src:
            pin = feature["properties"].get("PIN", "")
            if pin not in remaining_pins:
                continue

            geom = shapely.geometry.shape(feature["geometry"])
            features.append(
                _make_feature(pin, geom, remaining_pins[pin], transformer)
            )
            matched.add(pin)

    return features, matched


def build_parcels(
    zip_path: Path,
    pin_to_projects: dict[str, list[dict[str, Any]]],
    fallback_zips: list[Path] | None = None,
) -> dict:
    """Read parcels from shapefile zip, reproject, filter to matched PINs.

    Emits one feature per parcel with a `projects` array, and a top-level
    `status` for polygon coloring (highest-priority status among projects).

    After the primary shapefile pass, any unmatched PINs are searched in
    fallback_zips (historical snapshots, ordered newest-first).

    Args:
        zip_path: Path to parcels_shape_current.zip
        pin_to_projects: Mapping of PIN -> list of project dicts to attach
        fallback_zips: Optional list of historical parcel zips to try

    Returns:
        GeoJSON FeatureCollection dict
    """
    transformer = pyproj.Transformer.from_crs(
        "EPSG:2284", "EPSG:4326", always_xy=True
    )

    # Primary pass
    features, matched_pins = _match_pins_from_zip(
        zip_path, pin_to_projects, transformer
    )
    print(f"  Current shapefile: matched {len(matched_pins)} PINs")

    # Fallback passes through historical shapefiles
    if fallback_zips:
        remaining = {
            pin: projs
            for pin, projs in pin_to_projects.items()
            if pin not in matched_pins
        }
        for fb_zip in fallback_zips:
            if not remaining:
                break
            fb_features, fb_matched = _match_pins_from_zip(
                fb_zip, remaining, transformer
            )
            if fb_matched:
                features.extend(fb_features)
                matched_pins |= fb_matched
                for pin in fb_matched:
                    del remaining[pin]
                print(f"  {fb_zip.name}: matched {len(fb_matched)} more PINs")

    unmatched = set(pin_to_projects.keys()) - matched_pins
    multi = sum(1 for f in features if len(f["properties"]["projects"]) > 1)
    print(f"  Matched {len(matched_pins)} PINs total, {len(unmatched)} unmatched")
    print(f"  {len(features)} features ({multi} with multiple projects)")

    return {"type": "FeatureCollection", "features": features}
