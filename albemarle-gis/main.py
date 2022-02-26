#!/usr/bin/env python

"""Download historical property assessments from the Albemarle County GIS
website.
"""

import csv
import io
import logging
import pathlib
import re
import sys
import zipfile

import lxml.html
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

PARCEL_INFO_URL = (
    "https://gisweb.albemarle.org/gisdata/CAMA/GIS_View_Redacted_ParcelInfo_TXT.zip"
)
GIS_DETAIL_URL = "https://gisweb.albemarle.org/gpv_51/Services/SelectionPanel.ashx"
FIELD_NAMES = [
    "Assessment Date",
    "Land Value",
    "Land Use Value",
    "Improvements Value",
    "Total Value",
]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_session(retries=5):
    retry_strategy = Retry(total=retries)
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def get_parcel_info(session: requests.Session):
    """Get list of parcels from bulk downloads page."""
    response = session.get(PARCEL_INFO_URL, stream=True)
    response.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        with zf.open(zf.namelist()[0]) as fp:
            with io.TextIOWrapper(fp, encoding="utf-8") as buf:
                return list(csv.DictReader(buf))


def get_assessments(session: requests.Session, parcel_id: str):
    """Parse historical assessments from GIS HTML."""
    response = session.post(
        GIS_DETAIL_URL,
        data={
            "m": "GetDataListHtml",
            "datatab": "AssessmentInfo",
            "id": parcel_id,
        },
    )
    response.raise_for_status()
    doc = lxml.html.fromstring(response.content)
    row_sets = doc.xpath("//div[@class='RowSet']")
    for row_set in row_sets:
        header_texts = row_set.xpath(".//div[@class='RowSetHeader']/text()")
        if not header_texts:
            logger.warning("Possible duplicate assessment row")
        if header_texts and header_texts[0] == "Assessment History":
            continue
        pairs = [
            get_values(value_set)
            for value_set in row_set.xpath(".//div[@class='ValueSet']")
        ]
        yield {key: value for key, value in pairs if key.strip()}


def get_values(value_set):
    label = value_set.xpath("./div[@class='Label']/text()")[0]
    values = value_set.xpath("./div[@class='Value']/text()")
    if not values:
        logger.warning(f"Missing value for label {label}")
        value = ""
    else:
        value = values[0]
    if value.startswith("$"):
        value = value.replace("$", "").replace(",", "")
    elif date_match := re.search(r"(\d{2})/(\d{2})/(\d{4})", value):
        month, day, year = date_match.groups()
        value = "-".join([year, month, day])
    return label, value


if __name__ == "__main__":
    session = get_session()
    parcel_info = get_parcel_info(session)
    out_path = pathlib.Path("assessments.csv")
    if out_path.exists():
        logger.error(f"output path {out_path} exists")
        sys.exit(1)
    with out_path.open("w") as fp:
        writer = csv.DictWriter(fp, fieldnames=["ParcelID", *FIELD_NAMES])
        writer.writeheader()
        for row in parcel_info:
            logger.info(f"Checking parcel {row['ParcelID']}")
            assessments = get_assessments(session, row["ParcelID"])
            for assessment in assessments:
                writer.writerow(
                    {
                        **assessment,
                        "ParcelID": row["ParcelID"],
                    }
                )
