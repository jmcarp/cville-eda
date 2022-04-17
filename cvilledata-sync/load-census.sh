#!/bin/bash

set -euo pipefail

PROJECT_ID=cvilledata
DATASET_ID=census

source ./utils.sh

bq mk --force "${PROJECT_ID}:${DATASET_ID}"

curl -o scratch/tl_2010_51_tract10.zip https://www2.census.gov/geo/tiger/TIGER2010/TRACT/2010/tl_2010_51_tract10.zip
unzip scratch/tl_2010_51_tract10.zip -d scratch/tl_2010_51_tract10
geojsonify scratch/tl_2010_51_tract10 scratch/tracts-2010.csv
bq load --autodetect --replace "${PROJECT_ID}:${DATASET_ID}.census_tracts_2010_51" scratch/tracts-2010.csv

curl -o scratch/tl_2010_51_tabblock10.zip https://www2.census.gov/geo/tiger/TIGER2010/TABBLOCK/2010/tl_2010_51_tabblock10.zip
unzip scratch/tl_2010_51_tabblock10.zip -d scratch/tl_2010_51_tabblock10
geojsonify scratch/tl_2010_51_tabblock10 scratch/blocks-2010.csv
bq load --autodetect --replace "${PROJECT_ID}:${DATASET_ID}.census_blocks_2010_51" scratch/blocks-2010.csv

curl -o scratch/tl_2020_51_tract.zip https://www2.census.gov/geo/tiger/TIGER2020/TRACT/tl_2020_51_tract.zip
unzip scratch/tl_2020_51_tract.zip -d scratch/tl_2020_51_tract
geojsonify scratch/tl_2020_51_tract scratch/tracts-2020.csv
bq load --autodetect --replace "${PROJECT_ID}:${DATASET_ID}.census_tracts_2020_51" scratch/tracts-2020.csv

curl -o scratch/tl_2020_51_tabblock20.zip https://www2.census.gov/geo/tiger/TIGER2020/TABBLOCK20/tl_2020_51_tabblock20.zip
unzip scratch/tl_2020_51_tabblock20.zip -d scratch/tl_2020_51_tabblock20
geojsonify scratch/tl_2020_51_tabblock20 scratch/blocks-2020.csv
bq load --autodetect --replace "${PROJECT_ID}:${DATASET_ID}.census_blocks_2020_51" scratch/blocks-2020.csv
