#!/usr/bin/env python
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "httpx",
# ]
# ///
"""Download Albemarle County parcel shapefile."""

from pathlib import Path

import httpx

PARCELS_URL = (
    "https://albgis.albemarle.org/gisdata/Parcels/shape/parcels_shape_current.zip"
)
OUTPUT_PATH = Path(__file__).parent / "parcels.zip"


def main() -> int:
    print("Downloading parcel shapefile...")
    with httpx.stream("GET", PARCELS_URL, follow_redirects=True, timeout=120) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        downloaded = 0
        with open(OUTPUT_PATH, "wb") as f:
            for chunk in r.iter_bytes(chunk_size=65536):
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = downloaded * 100 // total
                    print(f"\r  {downloaded:,} / {total:,} bytes ({pct}%)", end="")
        print()

    print(f"Saved to {OUTPUT_PATH} ({downloaded:,} bytes)")
    return 0


if __name__ == "__main__":
    exit(main())
