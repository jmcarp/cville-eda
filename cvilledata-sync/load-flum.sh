#!/usr/bin/env bash

set -euo pipefail

PROJECT_ID=cvilledata
DATASET_ID=cville_plans_together

source ./utils.sh

bq mk --force "${PROJECT_ID}:${DATASET_ID}"

# Downloaded from https://drive.google.com/drive/folders/1ZBOIhKCjxlX0DCNs1xc8sd5HQPb3fDwK
unzip -o -d "cville/flum-202110" "FLUM Shapefile - October 2021 Draft.zip"
geojsonify "cville/flum-202110/FLUM Shapefile - October 2021 Draft" cville/flum-202110.csv
bq load --autodetect --replace --allow_quoted_newlines "${PROJECT_ID}:${DATASET_ID}.flum_202110" cville/flum-202110.csv
