#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import logging
import shelve
import os
import re

import lxml.html
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


BASE_URL = "https://api.walkscore.com"
BASE_URL_HTML = "https://www.walkscore.com"


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def main(api_key, in_path, shelf_path, csv_path, batch_size):
    session = requests.Session()
    retry = Retry(
        total=3,
        status_forcelist=[500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    rows = csv.DictReader(open(in_path))
    count = 0
    with shelve.open(shelf_path) as shelf:
        for row in rows:
            if row["gpin"] in shelf:
                continue
            logger.info(f"Looking up gpin {row['gpin']}: {row['lat']}, {row['lon']}")
            shelf[row["gpin"]] = lookup_html(session, row["lat"], row["lon"])
            shelf.sync()
            count += 1
            if count >= batch_size:
                return
        with open(csv_path, "w") as fp:
            writer = csv.DictWriter(fp, ["gpin", "walkscore", "transitscore", "bikescore"])
            writer.writeheader()
            for key, value in shelf.items():
                writer.writerow({
                    "gpin": key,
                    "walkscore": value["walk"],
                    "transitscore": value["transit"],
                    "bikescore": value["bike"],
                })


def lookup(session, api_key, latitude, longitude):
    """Look up scores from api.

    Note: some locations are missing transit or bike scores that are available
    using the html interface.
    """
    response = session.get(
        f"{BASE_URL}/score",
        params={
            "format": "json",
            "lat": latitude,
            "lon": longitude,
            "wsapikey": api_key,
            "transit": 1,
            "bike": 1,
        },
    )
    response.raise_for_status()
    data = response.json()
    if data["status"] != 1:
        raise RuntimeError(f"Got unexpected status code {data['status']}")
    return response.json()


def lookup_html(session, latitude, longitude):
    """Look up scores from html."""
    response = session.get(f"{BASE_URL_HTML}/score/loc/lat={latitude}/lng={longitude}")
    response.raise_for_status()
    doc = lxml.html.fromstring(response.content)
    return {
        "walk": extract_score(doc, "walk"),
        "transit": extract_score(doc, "transit"),
        "bike": extract_score(doc, "bike"),
    }


EXTRACT_PATTERN = re.compile("(\d+)\.svg$", re.IGNORECASE)


def extract_score(doc, mode):
    srcs = doc.xpath(f"//img[contains(@src, '/badge/{mode}/score/')]/@src")
    if len(srcs) > 0:
        match = EXTRACT_PATTERN.search(srcs[0])
        if match:
            score, = match.groups()
            return int(score)
    return None


if __name__ == "__main__":
    main(
        os.environ["WALKSCORE_API_KEY"],
        "parcel-centroids.csv",
        "parcel-walk-scores-html.shelf",
        "parcel-walk-scores.csv",
        15000,
    )
