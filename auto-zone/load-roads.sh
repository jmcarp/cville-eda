#!/bin/bash

set -euo pipefail

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
