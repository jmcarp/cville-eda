#!/usr/bin/env python
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "httpx",
# ]
# ///
"""Download Albemarle County parcel shapefiles (current + historical)."""

import sys
from pathlib import Path

import httpx

BASE_URL = "https://albgis.albemarle.org/gisdata/Parcels/shape"
CURRENT_URL = f"{BASE_URL}/parcels_shape_current.zip"
OUTPUT_PATH = Path(__file__).parent / "parcels.zip"

HISTORICAL_DIR = Path(__file__).parent / "parcels_historical"
# End-of-year snapshots that contain parcels not in later files.
# Skips years that contribute no additional matches (2001-2006, 2008-2012, 2014, 2022).
HISTORICAL_YEARS = [2024, 2023, 2021, 2020, 2019, 2018, 2017, 2016, 2015, 2013, 2007]


def download_file(url: str, dest: Path, label: str) -> int:
    """Download a file with progress, returning bytes written."""
    with httpx.stream("GET", url, follow_redirects=True, timeout=120) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        downloaded = 0
        with open(dest, "wb") as f:
            for chunk in r.iter_bytes(chunk_size=65536):
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = downloaded * 100 // total
                    print(f"\r  {label}: {downloaded:,} / {total:,} bytes ({pct}%)", end="")
        print()
    return downloaded


def fetch_current() -> int:
    """Download the current parcel shapefile."""
    print("Downloading current parcel shapefile...")
    downloaded = download_file(CURRENT_URL, OUTPUT_PATH, "parcels.zip")
    print(f"Saved to {OUTPUT_PATH} ({downloaded:,} bytes)")
    return 0


def fetch_historical() -> int:
    """Download historical parcel snapshots."""
    HISTORICAL_DIR.mkdir(exist_ok=True)
    for year in HISTORICAL_YEARS:
        dest = HISTORICAL_DIR / f"Parcels{year}.zip"
        if dest.exists():
            print(f"  Parcels{year}.zip already exists, skipping")
            continue
        url = f"{BASE_URL}/Parcels{year}.zip"
        print(f"Downloading Parcels{year}.zip...")
        try:
            downloaded = download_file(url, dest, f"Parcels{year}.zip")
            print(f"  Saved ({downloaded:,} bytes)")
        except httpx.HTTPStatusError as e:
            print(f"  Failed: {e.response.status_code} — skipping")
            dest.unlink(missing_ok=True)
    return 0


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] == "--historical":
        return fetch_historical()
    return fetch_current()


if __name__ == "__main__":
    exit(main())
