#!/bin/bash

set -euo pipefail

# Load Virginia counties to restrict road distance calculations
curl -O https://www2.census.gov/geo/tiger/TIGER2010/COUNTY/2010/tl_2010_51_county10.zip
unzip tl_2010_51_county10.zip -d tl_2010_51_county10
geojsonify tl_2010_51_county10 counties.csv
bq load --autodetect --replace whatthecarp:cville_eda_raw.census_counties counties.csv

# http://download.geofabrik.de/north-america/us/virginia.html
curl -O http://download.geofabrik.de/north-america/us/virginia-latest-free.shp.zip
unzip virginia-latest-free.shp.zip -d virginia-latest-free.shp 'gis_osm_roads_*'

mkdir -p roads
geojsonify virginia-latest-free.shp/gis_osm_roads_free_1.shp roads/local.csv 'select *, asgeojson(geometry) as geometry from gis_osm_roads_free_1 where fclass in (''primary'', ''secondary'', ''tertiary'')'
geojsonify virginia-latest-free.shp/gis_osm_roads_free_1.shp roads/primary.csv 'select *, asgeojson(geometry) as geometry from gis_osm_roads_free_1 where fclass = ''primary'''
geojsonify virginia-latest-free.shp/gis_osm_roads_free_1.shp roads/secondary.csv 'select *, asgeojson(geometry) as geometry from gis_osm_roads_free_1 where fclass = ''secondary'''
geojsonify virginia-latest-free.shp/gis_osm_roads_free_1.shp roads/tertiary.csv 'select *, asgeojson(geometry) as geometry from gis_osm_roads_free_1 where fclass = ''tertiary'''

bq load --autodetect --replace whatthecarp:cville_eda_raw.roads_local roads/local.csv
bq load --autodetect --replace whatthecarp:cville_eda_raw.roads_primary roads/primary.csv
bq load --autodetect --replace whatthecarp:cville_eda_raw.roads_secondary roads/secondary.csv
bq load --autodetect --replace whatthecarp:cville_eda_raw.roads_tertiary roads/tertiary.csv

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.roads_local_distance` as
with roads as (
  select
    roads.*
  from `whatthecarp.cville_eda_raw.roads_local` roads
  join `whatthecarp.cville_eda_raw.census_counties` counties
    on st_intersects(st_geogfromgeojson(roads.geometry), st_geogfromgeojson(counties.geometry))
  where counties.geoid10 = 51540 -- Charlottesville City
)
select
  * except (rank)
from (
  select
    details.objectid,
    details.parcelnumb as parcelnumber,
    roads.osm_id,
    roads.ref,
    st_distance(st_geogfromgeojson(details.geometry), st_geogfromgeojson(roads.geometry)) as distance,
    row_number() over (partition by details.parcelnumb order by st_distance(st_geogfromgeojson(details.geometry), st_geogfromgeojson(roads.geometry)) asc) as rank
  from `whatthecarp.cville_eda_raw.parcel_area_details` details
  cross join roads
)
where rank = 1'

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.roads_primary_distance` as
with roads as (
  select
    roads.*
  from `whatthecarp.cville_eda_raw.roads_primary` roads
  join `whatthecarp.cville_eda_raw.census_counties` counties
    on st_intersects(st_geogfromgeojson(roads.geometry), st_geogfromgeojson(counties.geometry))
  where counties.geoid10 = 51540 -- Charlottesville City
)
select
  * except (rank)
from (
  select
    details.objectid,
    details.parcelnumb as parcelnumber,
    roads.osm_id,
    roads.ref,
    st_distance(st_geogfromgeojson(details.geometry), st_geogfromgeojson(roads.geometry)) as distance,
    row_number() over (partition by details.parcelnumb order by st_distance(st_geogfromgeojson(details.geometry), st_geogfromgeojson(roads.geometry)) asc) as rank
  from `whatthecarp.cville_eda_raw.parcel_area_details` details
  cross join roads
)
where rank = 1'

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.roads_secondary_distance` as
with roads as (
  select
    roads.*
  from `whatthecarp.cville_eda_raw.roads_secondary` roads
  join `whatthecarp.cville_eda_raw.census_counties` counties
    on st_intersects(st_geogfromgeojson(roads.geometry), st_geogfromgeojson(counties.geometry))
  where counties.geoid10 = 51540 -- Charlottesville City
)
select
  * except (rank)
from (
  select
    details.objectid,
    details.parcelnumb as parcelnumber,
    roads.osm_id,
    roads.ref,
    st_distance(st_geogfromgeojson(details.geometry), st_geogfromgeojson(roads.geometry)) as distance,
    row_number() over (partition by details.parcelnumb order by st_distance(st_geogfromgeojson(details.geometry), st_geogfromgeojson(roads.geometry)) asc) as rank
  from `whatthecarp.cville_eda_raw.parcel_area_details` details
  cross join roads
)
where rank = 1'

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.roads_tertiary_distance` as
with roads as (
  select
    roads.*
  from `whatthecarp.cville_eda_raw.roads_tertiary` roads
  join `whatthecarp.cville_eda_raw.census_counties` counties
    on st_intersects(st_geogfromgeojson(roads.geometry), st_geogfromgeojson(counties.geometry))
  where counties.geoid10 = 51540 -- Charlottesville City
)
select
  * except (rank)
from (
  select
    details.objectid,
    details.parcelnumb as parcelnumber,
    roads.osm_id,
    roads.ref,
    st_distance(st_geogfromgeojson(details.geometry), st_geogfromgeojson(roads.geometry)) as distance,
    row_number() over (partition by details.parcelnumb order by st_distance(st_geogfromgeojson(details.geometry), st_geogfromgeojson(roads.geometry)) asc) as rank
  from `whatthecarp.cville_eda_raw.parcel_area_details` details
  cross join roads
)
where rank = 1'
