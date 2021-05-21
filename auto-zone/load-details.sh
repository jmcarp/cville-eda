#!/bin/bash

set -euo pipefail

# https://opendata.charlottesville.org/datasets/real-estate-residential-details
curl -o real-estate-residential-details.csv https://opendata.arcgis.com/datasets/c7adfdab73104a01a485dec324adcafb_17.csv
bq load --autodetect --replace whatthecarp:cville_eda_raw.real_estate_residential_details real-estate-residential-details.csv

# https://opendata.charlottesville.org/datasets/real-estate-commercial-details
curl -o real-estate-commercial-details.csv https://opendata.arcgis.com/datasets/17fbd0c459d84c71aa37b436d5231c0b_19.csv
bq load --autodetect --replace whatthecarp:cville_eda_raw.real_estate_commercial_details real-estate-commercial-details.csv
