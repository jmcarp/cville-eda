#!/usr/bin/env bash

set -euo pipefail

PROJECT_ID=cvilledata
DATASET_DERIVED_ID=cville_open_data_derived

source ./utils.sh

bq mk --force "${PROJECT_ID}:${DATASET_ID}"
bq mk --force "${PROJECT_ID}:${DATASET_DERIVED_ID}"

arcgis-hub \
  fetch-datasets \
  --group-id b793cfbc066d4fdc9ad44fe2885dcf67 \
  --tag 'property & land' \
  --path cville

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

bq query --nouse_legacy_sql < cville-open-data-derived.sql
