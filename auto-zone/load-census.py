#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
from typing import Dict, List

import requests


BASE_URL = "https://api.census.gov/data"
SF1_URL = "https://api.census.gov/data/2010/dec/sf1"
ACS_URL = "https://api.census.gov/data/{year}/acs/acs5"

VIRGINIA_FIPS = "51"
CHARLOTTESVILLE_FIPS = "540"
SF1_EXPORT_PATH = "sf1-export.csv"
ACS_BLOCKGROUP_EXPORT_PATH = "acs-blockgroup-export.csv"
ACS_BLOCKGROUP_BY_YEAR_EXPORT_PATH = "acs-blockgroup-by-year-export.csv"
ACS_TRACT_EXPORT_PATH = "acs-tract-export.csv"

SF1_VARIABLES = [
    "P003001",
    "P003002",
    "P003003",
    "P003005",
    "P005001",
    "P005003",
    "P005004",
    "P005010",
]

ACS_BLOCKGROUP_VARIABLES = [
    # Race
    "B02001_001E",
    "B02001_002E",
    "B02001_003E",
    "B02001_005E",
    # Income
    "B19013_001E",
    # Tenure
    "B25003_001E",
    "B25003_002E",
    # Education
    "B15003_001E",
    "B15003_021E",
    "B15003_022E",
    "B15003_023E",
    "B15003_024E",
    "B15003_025E",
]
ACS_TRACT_VARIABLES = [
    # School enrollment
    "B14001_001E",
    "B14001_008E",
    # Poverty status of families
    "B17012_001E",
    "B17012_002E",
]


def main():
    rows = fetch_sf1(SF1_VARIABLES)
    with open(SF1_EXPORT_PATH, "w") as fp:
        writer = csv.DictWriter(fp, fieldnames=rows[0].keys())
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    rows = fetch_acs(ACS_BLOCKGROUP_VARIABLES, "block group", 2019)
    with open(ACS_BLOCKGROUP_EXPORT_PATH, "w") as fp:
        writer = csv.DictWriter(fp, fieldnames=rows[0].keys())
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    rows = fetch_acs(ACS_TRACT_VARIABLES, "tract", 2019)
    with open(ACS_TRACT_EXPORT_PATH, "w") as fp:
        writer = csv.DictWriter(fp, fieldnames=rows[0].keys())
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    rows = []
    for year in range(2019, 2012, -1):
        batch = fetch_acs(ACS_BLOCKGROUP_VARIABLES, "block group", year)
        rows.extend([
            {**row, "year": year}
            for row in batch
        ])
    with open(ACS_BLOCKGROUP_BY_YEAR_EXPORT_PATH, "w") as fp:
        writer = csv.DictWriter(fp, fieldnames=rows[0].keys())
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def fetch_sf1(questions: List[str]) -> List[Dict]:
    response = requests.get(
        SF1_URL,
        params={
            "get": ",".join(["NAME", *questions]),
            "for": "block",
            "in": f"state:{VIRGINIA_FIPS} county:{CHARLOTTESVILLE_FIPS}",
        },
    )
    response.raise_for_status()
    raw = response.json()
    header, *rows = raw
    return [dict(zip(header, row)) for row in rows]


def fetch_acs(questions: List[str], for_: str, year: int) -> List[Dict]:
    response = requests.get(
        ACS_URL.format(year=year),
        params={
            "get": ",".join(["NAME", *questions]),
            "for": for_,
            "in": f"state:{VIRGINIA_FIPS} county:{CHARLOTTESVILLE_FIPS}",
        },
    )
    response.raise_for_status()
    raw = response.json()
    header, *rows = raw
    return [dict(zip(header, row)) for row in rows]


if __name__ == "__main__":
    main()
