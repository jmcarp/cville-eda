#!/usr/bin/env python
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "httpx",
# ]
# ///
"""Fetch parcel-to-zone mapping from Charlottesville Open Data."""

import argparse
import json
from pathlib import Path

import httpx

API_URL = "https://gisweb.charlottesville.org/arcgis/rest/services/OpenData_2/MapServer/20/query"


def fetch_parcels() -> dict[str, str]:
    """Fetch all parcels and return a ParcelNumber -> Zone mapping."""
    parcels = {}
    offset = 0
    batch_size = 1000

    with httpx.Client(timeout=60) as client:
        while True:
            resp = client.get(
                API_URL,
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

            print(f"Fetched {len(parcels)} parcels...")

            if not data.get("exceededTransferLimit", False):
                break

            offset += batch_size

    return parcels


def main():
    parser = argparse.ArgumentParser(description="Fetch parcel-to-zone mapping")
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=Path(__file__).parent / "parcels.json",
        help="Output JSON file",
    )
    args = parser.parse_args()

    print("Fetching parcels from Charlottesville Open Data...")
    parcels = fetch_parcels()
    print(f"Total parcels: {len(parcels)}")

    args.output.write_text(json.dumps(parcels, indent=2))
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
