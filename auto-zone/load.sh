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

# Load school parcels
bq load --schema 'name:STRING,parcelnumber:STRING' --skip_leading_rows 1 --replace whatthecarp:cville_eda_raw.school_parcels school-parcels.csv
bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.school_parcels` as
select
  school.*,
  details.geoparceli as gpin,
  details.* except(geoparceli)
from `whatthecarp.cville_eda_raw.school_parcels` school
join `whatthecarp.cville_eda_raw.parcel_area_details` details on school.parcelnumber = details.parcelnumb'

# Load park parcels
bq load --schema 'name:STRING,parcelnumber:STRING' --skip_leading_rows 1 --replace whatthecarp:cville_eda_raw.park_parcels park-parcels.csv
bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.park_parcels` as
select
  park.*,
  details.geoparceli as gpin,
  details.* except (geoparceli)
from `whatthecarp.cville_eda_raw.park_parcels` park
join `whatthecarp.cville_eda_raw.parcel_area_details` details on park.parcelnumber = details.parcelnumb'

# Load student blockgroups
bq load --schema 'blockgroup:STRING' --skip_leading_rows 1 --replace whatthecarp:cville_eda_raw.student_blockgroups student-blockgroups.csv

# Write parcels to geojson for visualization
ogr2ogr \
  -f GeoJSON \
  -dialect sqlite \
  parcel-area-details.geojson \
  parcel-area-details \
  -sql "$(cat <<EOF
select
  geoparceli as gpin,
  x(st_centroid(st_union(geometry))) as lat,
  y(st_centroid(st_union(geometry))) as lon
from parcel_area_details
group by geoparceli
EOF
)"

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.geopin` as
select
  geoparceli as gpin,
  count(parcelnumb) as parcels,
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

curl https://api.devhub.virginia.edu/v1/transit/bus-stops | jq -c '.stops | .[]' > uva-stops.ndjson
bq load --autodetect --replace --source_format NEWLINE_DELIMITED_JSON whatthecarp:cville_eda_raw.uva_stops uva-stops.ndjson

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.geopin_to_uts` as
select
  * except (rank)
from (
  select
    gpin.gpin,
    uts.id,
    uts.name,
    st_distance(gpin.geometry, st_geogpoint(uts.position[offset(1)], uts.position[offset(0)])) as distance,
    row_number() over (partition by gpin.gpin order by st_distance(gpin.geometry, st_geogpoint(uts.position[offset(1)], uts.position[offset(0)])) asc) as rank
  from `whatthecarp.cville_eda_derived.geopin` gpin
  cross join `whatthecarp.cville_eda_raw.uva_stops` uts
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
    park.name as parkname,
    st_distance(gpin.geometry, st_geogfromgeojson(park.geometry)) as distance,
    row_number() over (partition by gpin.gpin order by st_distance(gpin.geometry, st_geogfromgeojson(park.geometry)) asc) as rank
  from `whatthecarp.cville_eda_derived.geopin` gpin
  cross join `whatthecarp.cville_eda_derived.park_parcels` park
)
where rank = 1'

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.geopin_to_school` as
select
  * except (rank)
from (
  select
    gpin.gpin,
    school.name as schoolname,
    st_distance(gpin.geometry, st_geogfromgeojson(school.geometry)) as distance,
    row_number() over (partition by gpin.gpin order by st_distance(gpin.geometry, st_geogfromgeojson(school.geometry)) asc) as rank
  from `whatthecarp.cville_eda_derived.geopin` gpin
  cross join `whatthecarp.cville_eda_derived.school_parcels` school
)
where rank = 1'

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.value_by_geopin` as
select
  details.geoparceli as gpin,
  sum(assessments.landvalue) as landvalue,
  st_area(st_union_agg(st_geogfromgeojson(details.geometry))) as sqm,
  sum(assessments.landvalue) / st_area(st_union_agg(st_geogfromgeojson(details.geometry))) as landvaluepersqm,
from `whatthecarp.cville_eda_raw.parcel_area_details` details
join `whatthecarp.cville_eda_raw.real_estate_assessments` assessments on (details.parcelnumb = assessments.parcelnumber)
where assessments.taxyear = 2021
group by details.geoparceli'

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.value_by_block` as
select
  gpin_to_block.geoid10,
  avg(values.landvalue) as landvalue,
  avg(values.sqm) as sqm,
  avg(values.landvaluepersqm) as landvaluepersqm,
  percent_rank() over (order by avg(values.landvaluepersqm)) as landvaluepersqmrank,
from `whatthecarp.cville_eda_derived.value_by_geopin` values
join `whatthecarp.cville_eda_derived.geopin_to_block` gpin_to_block using (gpin)
where values.landvalue > 0
group by gpin_to_block.geoid10'
