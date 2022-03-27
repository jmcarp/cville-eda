#!/usr/bin/env bash

# Note: flood maps for Charlottesville are also available at the city ArcGIS REST service,
# but we use FEMA for accuracy and to generalize to other locations. For details, see
# https://gisweb.charlottesville.org/arcgis/rest/services/OpenData_1/MapServer/20

set -euo pipefail

PROJECT_ID=cvilledata
DATASET_ID=fema

source ./utils.sh

bq mk --force "${PROJECT_ID}:${DATASET_ID}"

# Download link from https://msc.fema.gov/portal/advanceSearch under "NFHL Data-County"
curl -L -o scratch/NFHL_51003C.zip 'https://hazards.fema.gov/nfhlv2/output/County/51003C_20211217.zip'
unzip -d scratch/NFHL_51003C scratch/NFHL_51003C.zip

# Extract full SFHA table
ogr2ogr \
  -f csv \
  -dialect sqlite \
  -sql "select *, asgeojson(geometry) as geometry from 'S_FLD_HAZ_AR'" \
  scratch/NFHL_51003C_SFHA.csv \
  scratch/NFHL_51003C

# Extract floodplain and floodway layers for convenience
ogr2ogr \
  -f csv \
  -dialect sqlite \
  -sql "select *, asgeojson(geometry) as geometry from 'S_FLD_HAZ_AR' where zone_subty is null" \
  scratch/NFHL_51003C_SFHA_floodplain_100_yr.csv \
  scratch/NFHL_51003C
ogr2ogr \
  -f csv \
  -dialect sqlite \
  -sql "select *, asgeojson(geometry) as geometry from 'S_FLD_HAZ_AR' where zone_subty = '0.2 PCT ANNUAL CHANCE FLOOD HAZARD'" \
  scratch/NFHL_51003C_SFHA_floodplain_500_yr.csv \
  scratch/NFHL_51003C
ogr2ogr \
  -f csv \
  -dialect sqlite \
  -sql "select *, asgeojson(geometry) as geometry from 'S_FLD_HAZ_AR' where zone_subty = 'FLOODWAY'" \
  scratch/NFHL_51003C_SFHA_floodway.csv \
  scratch/NFHL_51003C

bq load --autodetect --replace --allow_quoted_newlines "${PROJECT_ID}:${DATASET_ID}.sfha_51003C" scratch/NFHL_51003C_SFHA.csv
bq load --autodetect --replace --allow_quoted_newlines "${PROJECT_ID}:${DATASET_ID}.sfha_floodplain_100_yr_51003C" scratch/NFHL_51003C_SFHA_floodplain_100_yr.csv
bq load --autodetect --replace --allow_quoted_newlines "${PROJECT_ID}:${DATASET_ID}.sfha_floodplain_500_yr_51003C" scratch/NFHL_51003C_SFHA_floodplain_500_yr.csv
bq load --autodetect --replace --allow_quoted_newlines "${PROJECT_ID}:${DATASET_ID}.sfha_floodway_51003C" scratch/NFHL_51003C_SFHA_floodway.csv
