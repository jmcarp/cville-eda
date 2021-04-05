#!/bin/bash

set -euo pipefail

curl -O https://www2.census.gov/geo/tiger/TIGER2010/TABBLOCK/2010/tl_2010_51_tabblock10.zip
unzip tl_2010_51_tabblock10.zip -d tl_2010_51_tabblock10
ogr2ogr \
  -f csv \
  -dialect sqlite \
  -sql "select *, asgeojson(geometry) as geometry from tl_2010_51_tabblock10" \
  tl_2010_51_tabblock10.csv \
  tl_2010_51_tabblock10
bq load --autodetect --replace whatthecarp:cville_eda_raw.tl_2010_51_tabblock10 tl_2010_51_tabblock10.csv

curl -o parcel-area-details.zip https://opendata.arcgis.com/datasets/0e9946c2a77d4fc6ad16d9968509c588_72.zip
unzip parcel-area-details.zip -d parcel-area-details
ogr2ogr \
  -f csv \
  -dialect sqlite \
  -sql "select *, asgeojson(geometry) as geometry from parcel_area_details" \
  parcel-area-details.csv \
  parcel-area-details
bq load --autodetect --replace whatthecarp:cville_eda_raw.parcel_area_details parcel-area-details.csv

bq query --nouse_legacy_sql \
curl -o bus-stop-points.zip https://opendata.arcgis.com/datasets/6465cd54bcf4498495be8c86a9d7c3f2_4.zip
unzip bus-stop-points.zip -d bus-stop-points
ogr2ogr \
  -f csv \
  -dialect sqlite \
  -sql "select *, asgeojson(geometry) as geometry from cat_bus_stop_points" \
  bus-stop-points.csv \
  bus-stop-points
bq load --autodetect --replace whatthecarp:cville_eda_raw.bus_stop_points bus-stop-points.csv

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.parcel_to_block` as
select
  * except (rank)
from (
  select
    details.objectid,
    blocks.countyfp10,
    blocks.tractce10,
    blocks.blockce10,
    blocks.geoid10,
    rank() over (partition by details.objectid order by st_area(st_intersection(st_geogfromgeojson(details.geometry), st_geogfromgeojson(blocks.geometry))) desc) as rank
  from `whatthecarp.cville_eda_raw.parcel_area_details` details
  cross join `whatthecarp.cville_eda_raw.tl_2010_51_tabblock10` blocks
  where blocks.countyfp10 = 540 -- Charlottesville City
)
where rank = 1'

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.parcel_to_cat` as
select
  * except (rank)
from (
  select
    details.objectid,
    cat.stopid,
    st_distance(st_geogfromgeojson(details.geometry), st_geogfromgeojson(cat.geometry)) as distance,
    rank() over (partition by details.objectid order by st_distance(st_geogfromgeojson(details.geometry), st_geogfromgeojson(cat.geometry)) asc) as rank
  from `whatthecarp.cville_eda_raw.parcel_area_details` details
  cross join `whatthecarp.cville_eda_raw.bus_stop_points` cat
)
where rank = 1'
