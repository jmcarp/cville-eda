#!/bin/bash

set -euo pipefail

geojsonify() {
  local input=$1
  local output=$2
  local layer
  layer=$(ogrinfo ${input} | grep '1: ' | awk '{print $2}')
  local query="${3:-"select *, asgeojson(geometry) as geometry from ${layer}"}"
  ogr2ogr \
    -f csv \
    -dialect sqlite \
    -sql "${query}" \
    "${output}" \
    "${input}"
}

curl -O https://www2.census.gov/geo/tiger/TIGER2010/TABBLOCK/2010/tl_2010_51_tabblock10.zip
unzip tl_2010_51_tabblock10.zip -d tl_2010_51_tabblock10
geojsonify tl_2010_51_tabblock10 blocks.csv "select *, asgeojson(geometry) as geometry from tl_2010_51_tabblock10 where countyfp10 = '540'"
bq load --autodetect --replace whatthecarp:cville_eda_raw.census_blocks blocks.csv

curl -O https://www2.census.gov/geo/tiger/TIGER2010/TRACT/2010/tl_2010_51_tract10.zip
unzip tl_2010_51_tract10.zip -d tl_2010_51_tract10
geojsonify tl_2010_51_tract10 tracts.csv "select *, asgeojson(geometry) as geometry from tl_2010_51_tract10 where countyfp10 = '540'"
bq load --autodetect --replace whatthecarp:cville_eda_raw.census_tracts tracts.csv

curl -o parcel-area-details.zip https://opendata.arcgis.com/datasets/0e9946c2a77d4fc6ad16d9968509c588_72.zip
unzip parcel-area-details.zip -d parcel-area-details
geojsonify parcel-area-details parcel-area-details.csv "select distinct *, asgeojson(geometry) as geometry from parcel_area_details"
bq load --autodetect --replace whatthecarp:cville_eda_raw.parcel_area_details parcel-area-details.csv

# https://opendata.charlottesville.org/datasets/real-estate-base-data
curl -o real-estate-base.csv https://opendata.arcgis.com/datasets/bc72d0590bf940ff952ab113f10a36a8_8.csv
bq load --autodetect --replace whatthecarp:cville_eda_raw.real_estate_base real-estate-base.csv
bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_raw.real_estate_base` as
select distinct * except (recordid_int) from `whatthecarp.cville_eda_raw.real_estate_base`'

# https://opendata.charlottesville.org/datasets/real-estate-all-assessments
curl -o real-estate-assessments.csv https://opendata.arcgis.com/datasets/b993cd4e2e1b4ba097fb58c90725f5da_2.csv
bq load --autodetect --replace whatthecarp:cville_eda_raw.real_estate_assessments real-estate-assessments.csv
bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_raw.real_estate_assessments` as
select distinct * except (recordid_int) from `whatthecarp.cville_eda_raw.real_estate_assessments`'

# Write parcels to geojson for visualization
ogr2ogr \
  -f GeoJSON \
  -dialect sqlite \
  -sql "select distinct filetype, parcelnumb, streetname, streetnumb, unit, zoning, geometry from parcel_area_details" \
  parcel-area-details.geojson \
  parcel-area-details

curl -o bus-stop-points.zip https://opendata.arcgis.com/datasets/6465cd54bcf4498495be8c86a9d7c3f2_4.zip
unzip bus-stop-points.zip -d bus-stop-points
geojsonify bus-stop-points bus-stop-points.csv
bq load --autodetect --replace whatthecarp:cville_eda_raw.bus_stop_points bus-stop-points.csv

# https://opendata.charlottesville.org/datasets/park-area/
curl -o park-area.zip https://opendata.arcgis.com/datasets/a13bdf43fff04168b724a64f7dca234d_19.zip
unzip park-area.zip -d park-area
geojsonify park-area park-area.csv
bq load --autodetect --replace whatthecarp:cville_eda_raw.park_area park-area.csv

# Map parcels to tracts to simplify block mapping below
bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.parcel_to_tract` as
select
  * except (rank)
from (
  select
    details.parcelnumb as parcelnumber,
    tracts.countyfp10,
    tracts.tractce10,
    tracts.geoid10,
    row_number() over (partition by details.parcelnumb order by st_area(st_intersection(st_geogfromgeojson(details.geometry), st_geogfromgeojson(tracts.geometry))) desc) as rank
  from `whatthecarp.cville_eda_raw.parcel_area_details` details
  cross join `whatthecarp.cville_eda_raw.census_tracts` tracts
)
where rank = 1'

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.parcel_to_block` as
select
  * except (rank)
from (
  select
    details.parcelnumb as parcelnumber,
    blocks.countyfp10,
    blocks.tractce10,
    blocks.blockce10,
    blocks.geoid10,
    row_number() over (partition by details.parcelnumb order by st_area(st_intersection(st_geogfromgeojson(details.geometry), st_geogfromgeojson(blocks.geometry))) desc) as rank
  from `whatthecarp.cville_eda_raw.parcel_area_details` details
  join `whatthecarp.cville_eda_derived.parcel_to_tract` tracts on details.parcelnumb = tracts.parcelnumber
  join `whatthecarp.cville_eda_raw.census_blocks` blocks on tracts.tractce10 = blocks.tractce10
)
where rank = 1'

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.parcel_to_cat` as
select
  * except (rank)
from (
  select
    details.parcelnumb as parcelnumber,
    cat.stopid,
    st_distance(st_geogfromgeojson(details.geometry), st_geogfromgeojson(cat.geometry)) as distance,
    row_number() over (partition by details.parcelnumb order by st_distance(st_geogfromgeojson(details.geometry), st_geogfromgeojson(cat.geometry)) asc) as rank
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
    details.parcelnumb as parcelnumber,
    park.objectid as parkid,
    st_distance(st_geogfromgeojson(details.geometry), st_geogfromgeojson(park.geometry)) as distance,
    row_number() over (partition by details.parcelnumb order by st_distance(st_geogfromgeojson(details.geometry), st_geogfromgeojson(park.geometry)) asc) as rank
  from `whatthecarp.cville_eda_raw.parcel_area_details` details
  cross join `whatthecarp.cville_eda_raw.park_area` park
)
where rank = 1'

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.value_by_block` as
select distinct
  parcel_to_block.geoid10,
  percentile_cont(assessments.landvalue, 0.5) over (partition by parcel_to_block.geoid10) as landvalue,
  percentile_cont(assessments.landvalue / base.acreage, 0.5) over (partition by parcel_to_block.geoid10) as landvalueperacre,
from `whatthecarp.cville_eda_raw.parcel_area_details` details
join `whatthecarp.cville_eda_raw.real_estate_base` base on (details.parcelnumb = base.parcelnumber)
join `whatthecarp.cville_eda_raw.real_estate_assessments` assessments on (details.parcelnumb = assessments.parcelnumber)
join `whatthecarp.cville_eda_derived.parcel_to_block` parcel_to_block on (details.parcelnumb = parcel_to_block.parcelnumber)
where details.filetype = "R"
  and base.acreage != 0
  and assessments.landvalue != 0'
