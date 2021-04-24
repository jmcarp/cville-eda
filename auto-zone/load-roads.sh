#!/bin/bash

set -euo pipefail

# Load roads from OSM

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
bq load --autodetect --replace whatthecarp:cville_eda_raw.roads_local roads/local.csv

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.roads_local` as
select
  roads.*
from `whatthecarp.cville_eda_raw.roads_local` roads
join `whatthecarp.cville_eda_raw.census_counties` counties
  on st_intersects(st_geogfromgeojson(roads.geometry), st_geogfromgeojson(counties.geometry))
where counties.geoid10 = 51540 -- Charlottesville City'

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.geopin_to_roads_local` as
select
  * except (rank)
from (
  select
    gpin.gpin,
    roads.osm_id,
    roads.ref,
    st_distance(gpin.geometry, st_geogfromgeojson(roads.geometry)) as distance,
    row_number() over (partition by gpin.gpin order by st_distance(gpin.geometry, st_geogfromgeojson(roads.geometry)) asc) as rank
  from `whatthecarp.cville_eda_derived.geoparcelid` gpin
  cross join `whatthecarp.cville_eda_derived.roads_local` roads
)
where rank = 1'

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.geopin_to_roads_primary` as
select
  * except (rank)
from (
  select
    gpin.gpin,
    roads.osm_id,
    roads.ref,
    st_distance(gpin.geometry, st_geogfromgeojson(roads.geometry)) as distance,
    row_number() over (partition by gpin.gpin order by st_distance(gpin.geometry, st_geogfromgeojson(roads.geometry)) asc) as rank
  from `whatthecarp.cville_eda_derived.geoparcelid` gpin
  cross join `whatthecarp.cville_eda_derived.roads_local` roads
  where roads.fclass = "primary"
)
where rank = 1'

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.geopin_to_roads_secondary` as
select
  * except (rank)
from (
  select
    gpin.gpin,
    roads.osm_id,
    roads.ref,
    st_distance(gpin.geometry, st_geogfromgeojson(roads.geometry)) as distance,
    row_number() over (partition by gpin.gpin order by st_distance(gpin.geometry, st_geogfromgeojson(roads.geometry)) asc) as rank
  from `whatthecarp.cville_eda_derived.geoparcelid` gpin
  cross join `whatthecarp.cville_eda_derived.roads_local` roads
  where roads.fclass = "secondary"
)
where rank = 1'

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.geopin_to_roads_tertiary` as
select
  * except (rank)
from (
  select
    gpin.gpin,
    roads.osm_id,
    roads.ref,
    st_distance(gpin.geometry, st_geogfromgeojson(roads.geometry)) as distance,
    row_number() over (partition by gpin.gpin order by st_distance(gpin.geometry, st_geogfromgeojson(roads.geometry)) asc) as rank
  from `whatthecarp.cville_eda_derived.geoparcelid` gpin
  cross join `whatthecarp.cville_eda_derived.roads_local` roads
  where roads.fclass = "tertiary"
)
where rank = 1'

# Load roads from city data portal

# https://opendata.charlottesville.org/datasets/road-centerlines
curl -o road-centerlines.zip https://opendata.arcgis.com/datasets/5ea50546852444a890dc55c9d68104f8_29.zip
unzip road-centerlines.zip -d road-centerlines
geojsonify road-centerlines road-centerlines.csv 'select *, asgeojson(geometry) as geometry from road_centerlines where st_class in (''PA'', ''MA'', ''CO'')'
bq load --autodetect --replace whatthecarp:cville_eda_raw.road_centerlines road-centerlines.csv

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.geopin_to_roads` as
select
  * except (rank)
from (
  select
    gpin.gpin,
    roads.objectid,
    st_distance(gpin.geometry, st_geogfromgeojson(roads.geometry)) as distance,
    row_number() over (partition by gpin.gpin order by st_distance(gpin.geometry, st_geogfromgeojson(roads.geometry)) asc) as rank
  from `whatthecarp.cville_eda_derived.geopin` gpin
  cross join `whatthecarp.cville_eda_raw.road_centerlines` roads
)
where rank = 1'
