#!/bin/bash

set -euo pipefail

geojsonify() {
  local input=$1
  local output=$2
  local layer
  layer=$(ogrinfo ${input} | grep '1: ' | awk '{print $2}')
  ogr2ogr \
    -f csv \
    -dialect sqlite \
    -sql "select *, asgeojson(geometry) as geometry from ${layer}" \
    "${output}" \
    "${input}"
}

curl -O https://www2.census.gov/geo/tiger/TIGER2010/TABBLOCK/2010/tl_2010_51_tabblock10.zip
unzip tl_2010_51_tabblock10.zip -d tl_2010_51_tabblock10
ogr2ogr \
  -f csv \
  -dialect sqlite \
  -sql "select *, asgeojson(geometry) as geometry from tl_2010_51_tabblock10 where countyfp10 = '540'" \
  blocks.csv \
  tl_2010_51_tabblock10
bq load --autodetect --replace whatthecarp:cville_eda_raw.census_blocks blocks.csv

curl -o parcel-area-details.zip https://opendata.arcgis.com/datasets/0e9946c2a77d4fc6ad16d9968509c588_72.zip
unzip parcel-area-details.zip -d parcel-area-details
geojsonify parcel-area-details parcel-area-details.csv
bq load --autodetect --replace whatthecarp:cville_eda_raw.parcel_area_details parcel-area-details.csv

curl -o bus-stop-points.zip https://opendata.arcgis.com/datasets/6465cd54bcf4498495be8c86a9d7c3f2_4.zip
unzip bus-stop-points.zip -d bus-stop-points
geojsonify bus-stop-points bus-stop-points.csv
bq load --autodetect --replace whatthecarp:cville_eda_raw.bus_stop_points bus-stop-points.csv

# https://opendata.charlottesville.org/datasets/park-area/
curl -o park-area.zip https://opendata.arcgis.com/datasets/a13bdf43fff04168b724a64f7dca234d_19.zip
unzip park-area.zip -d park-area
geojsonify park-area park-area.csv
bq load --autodetect --replace whatthecarp:cville_eda_raw.park_area park-area.csv

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.parcel_to_block` as
select
  * except (rank)
from (
  select
    details.objectid,
    details.parcelnumb as parcelnumber,
    blocks.countyfp10,
    blocks.tractce10,
    blocks.blockce10,
    blocks.geoid10,
    rank() over (partition by details.objectid order by st_area(st_intersection(st_geogfromgeojson(details.geometry), st_geogfromgeojson(blocks.geometry))) desc) as rank
  from `whatthecarp.cville_eda_raw.parcel_area_details` details
  cross join `whatthecarp.cville_eda_raw.census_blocks` blocks
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

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.parcel_to_park` as
select
  * except (rank)
from (
  select
    details.objectid,
    park.objectid as parkid,
    st_distance(st_geogfromgeojson(details.geometry), st_geogfromgeojson(park.geometry)) as distance,
    rank() over (partition by details.objectid order by st_distance(st_geogfromgeojson(details.geometry), st_geogfromgeojson(park.geometry)) asc) as rank
  from `whatthecarp.cville_eda_raw.parcel_area_details` details
  cross join `whatthecarp.cville_eda_raw.park_area` park
)
where rank = 1'

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.value_by_block` as
with details as (
  select
    *,
    rank() over (partition by parcelnumb order by objectid) as rank
  from `whatthecarp.cville_eda_raw.parcel_area_details`
), base as (
  select
    *,
    rank() over (partition by parcelnumber order by recordid_int) as rank
  from `whatthecarp.cville_eda_raw.real_estate_base`
), assessments as (
  select
    *,
    rank() over (partition by parcelnumber order by recordid_int) as rank
  from `whatthecarp.cville_eda_raw.real_estate_assessments`
)
select distinct
  parcel_to_block.geoid10,
  percentile_cont(assessments.landvalue, 0.5) over (partition by parcel_to_block.geoid10) as landvalue,
  percentile_cont(assessments.landvalue / base.acreage, 0.5) over (partition by parcel_to_block.geoid10) as landvalueperacre,
from details
join base on (details.parcelnumb = base.parcelnumber)
join assessments on (details.parcelnumb = assessments.parcelnumber)
join `whatthecarp.cville_eda_derived.parcel_to_block` parcel_to_block on (details.parcelnumb = parcel_to_block.parcelnumber)
where details.rank = 1
  and base.rank = 1
  and assessments.rank = 1
  and details.filetype = 'R'
  and base.acreage != 0
  and assessments.landvalue != 0'
