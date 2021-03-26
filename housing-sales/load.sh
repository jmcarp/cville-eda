#!/bin/sh

set -euo pipefail

# https://opendata.charlottesville.org/datasets/real-estate-sales
curl -o real-estate-sales.csv https://opendata.arcgis.com/datasets/489adf140c174534a544136dc3e4cb90_3.csv

# https://opendata.charlottesville.org/datasets/parcel-area-details
curl -o parcel-area-details.csv https://opendata.arcgis.com/datasets/0e9946c2a77d4fc6ad16d9968509c588_72.csv

bq load --autodetect --replace whatthecarp:cville_eda_raw.real_estate_sales real-estate-sales.csv
bq load --autodetect --replace whatthecarp:cville_eda_raw.parcel_area_details parcel-area-details.csv
