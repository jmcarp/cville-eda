#!/bin/bash

set -euo pipefail

PROJECT_ID=cvilledata
DATASET_ID=census

source ./utils.sh

bq mk --force "${PROJECT_ID}:${DATASET_ID}"

curl -o scratch/tl_2010_51_tract10.zip https://www2.census.gov/geo/tiger/TIGER2010/TRACT/2010/tl_2010_51_tract10.zip
unzip scratch/tl_2010_51_tract10.zip -d scratch/tl_2010_51_tract10
geojsonify scratch/tl_2010_51_tract10 scratch/tracts.csv
bq load --autodetect --replace "${PROJECT_ID}:${DATASET_ID}.census_tracts_2010_51" scratch/tracts.csv

curl -o scratch/tl_2010_51_tabblock10.zip https://www2.census.gov/geo/tiger/TIGER2010/TABBLOCK/2010/tl_2010_51_tabblock10.zip
unzip scrach/tl_2010_51_tabblock10.zip -d scrach/tl_2010_51_tabblock10
geojsonify scratch/tl_2010_51_tabblock10 scratch/blocks.csv
bq load --autodetect --replace "${PROJECT_ID}:${DATASET_ID}.census_blocks_2010_51" scratch/blocks.csv
