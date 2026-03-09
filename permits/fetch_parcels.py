#!/usr/bin/env python
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "httpx",
# ]
# ///
"""Fetch parcel data from Charlottesville Open Data.

Two endpoints:
- Layer 20 (OpenData_2): ParcelNumber -> Zone table (no geometry).
- Layer 72 (OpenData_1): Parcel polygons with ParcelNumber + Zoning.
"""

import argparse
import json
from pathlib import Path

import httpx

ZONE_API_URL = (
    "https://gisweb.charlottesville.org/arcgis/rest/services"
    "/OpenData_2/MapServer/20/query"
)
GEOMETRY_URL = (
    "https://opendata.arcgis.com/datasets"
    "/0e9946c2a77d4fc6ad16d9968509c588_72.geojson"
)

BASE_DIR = Path(__file__).parent


def fetch_parcels() -> dict[str, str]:
    """Fetch all parcels and return a ParcelNumber -> Zone mapping."""
    parcels = {}
    offset = 0
    batch_size = 1000

    with httpx.Client(timeout=60) as client:
        while True:
            resp = client.get(
                ZONE_API_URL,
                params={
                    "where": "1=1",
                    "outFields": "ParcelNumber,Zone",
                    "returnGeometry": "false",
                    "resultOffset": offset,
                    "resultRecordCount": batch_size,
                    "f": "json",
                },
            )
            resp.raise_for_status()
            data = resp.json()

            features = data.get("features", [])
            if not features:
                break

            for feature in features:
                attrs = feature.get("attributes", {})
                parcel_num = attrs.get("ParcelNumber")
                zone = attrs.get("Zone")
                if parcel_num and zone:
                    parcels[parcel_num] = zone

            print(f"  Zones: {len(parcels)} parcels...")

            if not data.get("exceededTransferLimit", False):
                break

            offset += batch_size

    return parcels


def fetch_geometry(output: Path) -> int:
    """Download parcel polygons GeoJSON from ArcGIS Hub (single request).

    Returns the number of features downloaded.
    """
    downloaded = 0
    with httpx.stream("GET", GEOMETRY_URL, follow_redirects=True, timeout=120) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        with open(output, "wb") as f:
            for chunk in r.iter_bytes(chunk_size=65536):
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = downloaded * 100 // total
                    print(f"\r  {downloaded:,} / {total:,} bytes ({pct}%)", end="")
        print()

    # Count features for reporting
    with open(output) as f:
        data = json.load(f)
    return len(data.get("features", []))


def main():
    parser = argparse.ArgumentParser(description="Fetch Charlottesville parcel data")
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=BASE_DIR / "parcels.json",
        help="Output JSON file for zone mapping",
    )
    parser.add_argument(
        "--geometry-output",
        type=Path,
        default=BASE_DIR / "parcels_geo.geojson",
        help="Output GeoJSON file for parcel polygons",
    )
    parser.add_argument(
        "--skip-zones",
        action="store_true",
        help="Skip fetching zone mapping (layer 20)",
    )
    parser.add_argument(
        "--skip-geometry",
        action="store_true",
        help="Skip fetching geometry (layer 72)",
    )
    args = parser.parse_args()

    if not args.skip_zones:
        print("Fetching zone mapping from layer 20...")
        parcels = fetch_parcels()
        print(f"Total zone mappings: {len(parcels)}")
        args.output.write_text(json.dumps(parcels, indent=2))
        print(f"Wrote {args.output}")

    if not args.skip_geometry:
        print("Fetching parcel geometry from layer 72...")
        count = fetch_geometry(args.geometry_output)
        size_mb = args.geometry_output.stat().st_size / (1024 * 1024)
        print(f"Total features: {count} ({size_mb:.1f} MB)")
        print(f"Wrote {args.geometry_output}")


if __name__ == "__main__":
    main()
