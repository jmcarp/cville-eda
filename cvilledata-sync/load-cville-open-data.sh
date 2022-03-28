#!/usr/bin/env bash

set -euo pipefail

PROJECT_ID=cvilledata
DATASET_ID=cville_open_data
DATASET_DERIVED_ID=cville_open_data_derived
GROUP_ID=b793cfbc066d4fdc9ad44fe2885dcf67
MISC_DATASET_IDS=(
  "f4efb475a1ca4b919fca4645b72fadd0_76"  # Sidewalks
  "5ea50546852444a890dc55c9d68104f8_29"  # Road Centerlines
  "fa6e17734a784cadbe40a3d9cf674766_30"  # Road Area
  "f4efb475a1ca4b919fca4645b72fadd0_33"  # Bodies of Water
)

source ./utils.sh

bq mk --force "${PROJECT_ID}:${DATASET_ID}"
bq mk --force "${PROJECT_ID}:${DATASET_DERIVED_ID}"

arcgis-hub fetch-datasets --group-id "${GROUP_ID}" --tag 'property & land' --path cville

for dataset_id in "${MISC_DATASET_IDS[@]}"; do
  arcgis-hub fetch-datasets-by-id --dataset-id "${dataset_id}" --path cville
done

for dataset in cville/*.zip; do
  label=$(echo "${dataset}" | \
    tr '[:upper:]' '[:lower:]' | \
    sed 's/^cville//' | \
    sed 's/\.zip$//' | \
    sed 's/[^a-z0-9]/_/g' | \
    tr -s '_' | \
    sed 's/^_//' | \
    sed 's/_$//'
  )
  unzip -o -d "cville/${label}" "${dataset}"
  geojsonify "cville/${label}" "cville/${label}.csv"
  bq load --autodetect --replace --allow_quoted_newlines "${PROJECT_ID}:${DATASET_ID}.${label}" "cville/${label}.csv"
done

./get-arcgis-raster.py --layer CriticalSlopeLotRegulation --outfile cville/critical-slopes.csv
bq load --autodetect --replace "${PROJECT_ID}:${DATASET_ID}.critical_slope_log_regulation" cville/critical-slopes.csv

bq query --nouse_legacy_sql < cville-open-data-derived.sql
