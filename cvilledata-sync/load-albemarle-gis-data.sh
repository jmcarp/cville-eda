#!/usr/bin/env bash

set -euo pipefail

PROJECT_ID=cvilledata
DATASET_ID=albemarle_gis_data

source ./utils.sh

rm -rf scratch
mkdir -p scratch

bq mk --force "${PROJECT_ID}:${DATASET_ID}"

update_shapefile() {
  local label=$1
  local url=$2
  curl -o "scratch/${label}.zip" "${url}"
  unzip -d "scratch/${label}" "scratch/${label}.zip"
  geojsonify "scratch/${label}" "scratch/${label}.csv"
  bq load --autodetect --replace "${PROJECT_ID}:${DATASET_ID}.${label}" "scratch/${label}.csv"
}

update_shapefile county_parcels https://gisweb.albemarle.org/gisdata/Property/county_parcels.zip
update_shapefile current_parcels https://gisweb.albemarle.org/gisdata/Parcels/shape/parcels_shape_current.zip
update_shapefile subdivisions https://gisweb.albemarle.org/gisdata/Subdivisions/subdivisions_2021-05.zip

update_csv() {
  local label=$1
  local url=$2
  curl -o "scratch/${label}.zip" "${url}"
  unzip -d "scratch/${label}" "scratch/${label}.zip"
  # Standardize missing values
  sed -i 's/N\/A//g' scratch/"${label}"/*.txt
  sed -i 's/NULL//g' scratch/"${label}"/*.txt
  bq load --autodetect --replace "${PROJECT_ID}:${DATASET_ID}.${label}" scratch/"${label}"/*.txt
}

update_csv parcel_level_data https://gisweb.albemarle.org/gisdata/CAMA/GIS_View_Redacted_ParcelInfo_TXT.zip
update_csv card_level_data https://gisweb.albemarle.org/gisdata/CAMA/GIS_CardLevelData_new_TXT.zip
update_csv parcel_characteristics https://gisweb.albemarle.org/gisdata/CAMA/CityView_View_OtherParcelCharacteristics_TXT.zip

# Handle edge casees in transfers table:
# - Handle errant backticks in integer values
# - Set schema explicitly to fix auto-detection errors
curl -o scratch/transfer_history.zip https://gisweb.albemarle.org/gisdata/CAMA/GIS_View_Redacted_VisionSales_TXT.zip
unzip -d scratch/transfer_history scratch/transfer_history.zip
sed -i 's/N\/A//g' scratch/transfer_history/*.txt
sed -i 's/NULL//g' scratch/transfer_history/*.txt
sed -r -i 's/`([0-9]+)/\1/g' scratch/transfer_history/*.txt
bq load \
  --replace \
  --schema mapblolot:STRING,currowner:STRING,saledate1:STRING,saleprice:INT64,deedbook:STRING,deedpage:STRING,validitycode:STRING \
  --skip_leading_rows 1 \
  "${PROJECT_ID}:${DATASET_ID}.transfer_history" \
  scratch/transfer_history/*.txt
