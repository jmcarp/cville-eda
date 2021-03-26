#!/bin/bash

set -euo pipefail

curl -O https://www2.census.gov/geo/tiger/TIGER2010/TABBLOCK/2010/tl_2010_51_tabblock10.zip
mkdir tl_2010_51_tabblock10
unzip tl_2010_51_tabblock10.zip -d tl_2010_51_tabblock10

ogr2ogr \
    -f csv \
    -dialect sqlite \
    tl_2010_51_tabblock10.csv \
    tl_2010_51_tabblock10 \
    -sql "$(cat <<EOF
select
    asgeojson(st_centroid(geometry)) as centroid,
    statefp10,
    countyfp10,
    tractce10,
    blockce10,
    geoid10,
    name10,
    mtfcc10,
    ur10,
    uace10
    uatyp10,
    funcstat10,
    aland10,
    awater10
from tl_2010_51_tabblock10
EOF
)"

gsutil cp va_od_main_JT00_2018.csv gs://stage-airflow-sync/
bq load --autodetect --replace demsstaff:sbx_carpj.va_od_main_JT00_2018 gs://stage-airflow-sync/va_od_main_JT00_2018.csv
bq load --autodetect --replace demsstaff:sbx_carpj.va_od_main_JT00_2018 va_od_main_JT00_2018.csv
