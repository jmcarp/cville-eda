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
  parcel-area-details.geojson \
  parcel-area-details \
  -sql "$(cat <<EOF
select
  geoparceli as gpin,
  group_concat(parcelnumb, ', ') as parcelnumbers,
  group_concat(address, ', ') as addresses,
  st_union(geometry) as geometry
from (
  select
    *,
    coalesce(streetnumb, '?') || ' ' || streetname as address
  from parcel_area_details
)
group by geoparceli
EOF
)"

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.geopin` as
select
  geoparceli as gpin,
  array_agg(
    distinct concat(coalesce(streetnumb, "?"), " ", streetname)
    order by concat(coalesce(streetnumb, "?"), " ", streetname)
  ) as addresses,
  st_union_agg(st_geogfromgeojson(geometry)) as geometry
from `whatthecarp.cville_eda_raw.parcel_area_details`
group by geoparceli'

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
'create or replace table `whatthecarp.cville_eda_derived.geopin_to_tract` as
select
  * except (rank)
from (
  select
    gpin.gpin,
    tracts.countyfp10,
    tracts.tractce10,
    tracts.geoid10,
    row_number() over (partition by gpin.gpin order by st_area(st_intersection(gpin.geometry, st_geogfromgeojson(tracts.geometry))) desc) as rank
  from `whatthecarp.cville_eda_derived.geopin` gpin
  cross join `whatthecarp.cville_eda_raw.census_tracts` tracts
)
where rank = 1'

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.geopin_to_block` as
select
  * except (rank)
from (
  select
    gpin.gpin,
    blocks.countyfp10,
    blocks.tractce10,
    blocks.blockce10,
    blocks.geoid10,
    row_number() over (partition by gpin.gpin order by st_area(st_intersection(gpin.geometry, st_geogfromgeojson(blocks.geometry))) desc) as rank
  from `whatthecarp.cville_eda_derived.geopin` gpin
  join `whatthecarp.cville_eda_derived.geopin_to_tract` tracts on gpin.gpin = tracts.gpin
  join `whatthecarp.cville_eda_raw.census_blocks` blocks on tracts.tractce10 = blocks.tractce10
)
where rank = 1'

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.geopin_to_cat` as
select
  * except (rank)
from (
  select
    gpin.gpin,
    cat.stopid,
    st_distance(gpin.geometry, st_geogfromgeojson(cat.geometry)) as distance,
    row_number() over (partition by gpin.gpin order by st_distance(gpin.geometry, st_geogfromgeojson(cat.geometry)) asc) as rank
  from `whatthecarp.cville_eda_derived.geopin` gpin
  cross join `whatthecarp.cville_eda_raw.bus_stop_points` cat
)
where rank = 1'

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.geopin_to_park` as
select
  * except (rank)
from (
  select
    gpin.gpin,
    park.objectid as parkid,
    st_distance(gpin.geometry, st_geogfromgeojson(park.geometry)) as distance,
    row_number() over (partition by gpin.gpin order by st_distance(gpin.geometry, st_geogfromgeojson(park.geometry)) asc) as rank
  from `whatthecarp.cville_eda_derived.geopin` gpin
  cross join `whatthecarp.cville_eda_raw.park_area` park
)
where rank = 1'

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.value_by_block` as
with values as (
  select
    details.geoparceli as gpin,
    sum(assessments.landvalue) as landvalue,
    sum(assessments.landvalue) / st_area(st_union_agg(st_geogfromgeojson(details.geometry))) as landvaluepersqm
  from `whatthecarp.cville_eda_raw.parcel_area_details` details
  join `whatthecarp.cville_eda_raw.real_estate_assessments` assessments on (details.parcelnumb = assessments.parcelnumber)
  where assessments.taxyear = 2021
  group by details.geoparceli
)
select distinct
  gpin_to_block.geoid10,
  percentile_cont(values.landvalue, 0.5) over (partition by gpin_to_block.geoid10) as landvalue,
  percentile_cont(values.landvaluepersqm, 0.5) over (partition by gpin_to_block.geoid10) as landvaluepersqm,
from values
join `whatthecarp.cville_eda_derived.geopin_to_block` gpin_to_block using (gpin)
where values.landvalue > 0'
