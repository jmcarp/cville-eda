#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
from typing import Dict, List

import requests


BASE_URL = "https://api.census.gov/data"
SF1_URL = "https://api.census.gov/data/2010/dec/sf1"
ACS_URL = "https://api.census.gov/data/2019/acs/acs5"

"2010/dec/sf1"
VIRGINIA_FIPS = "51"
CHARLOTTESVILLE_FIPS = "540"
SF1_RACE_PATH = "sf1-race.csv"
ACS_RACE_PATH = "acs-race.csv"


def main():
    sf1_rows = fetch_sf1("P003001", "P003002", "P003003", "P005001", "P005003", "P005004", "P005010")
    with open(SF1_RACE_PATH, "w") as fp:
        writer = csv.DictWriter(fp, fieldnames=sf1_rows[0].keys())
        writer.writeheader()
        for row in sf1_rows:
            writer.writerow(row)

    acs_rows = fetch_acs("B02001_001E", "B02001_002E", "B02001_003E")
    with open(ACS_RACE_PATH, "w") as fp:
        writer = csv.DictWriter(fp, fieldnames=acs_rows[0].keys())
        writer.writeheader()
        for row in acs_rows:
            writer.writerow(row)


def fetch_sf1(*questions) -> List[Dict]:
    response = requests.get(
        SF1_URL,
        params={
            "get": ",".join([*questions, "NAME"]),
            "for": "block",
            "in": f"state:{VIRGINIA_FIPS} county:{CHARLOTTESVILLE_FIPS}",
        },
    )
    response.raise_for_status()
    raw = response.json()
    header, *rows = raw
    return [dict(zip(header, row)) for row in rows]


def fetch_acs(*questions) -> List[Dict]:
    response = requests.get(
        ACS_URL,
        params={
            "get": ",".join([*questions, "NAME"]),
            "for": "block group",
            "in": f"state:{VIRGINIA_FIPS} county:{CHARLOTTESVILLE_FIPS}",
        },
    )
    response.raise_for_status()
    raw = response.json()
    header, *rows = raw
    return [dict(zip(header, row)) for row in rows]


if __name__ == "__main__":
    main()
