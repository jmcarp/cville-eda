#!/bin/sh

set -euo pipefail

# Write parcel centroids to csv
ogr2ogr \
  -f csv \
  -dialect sqlite \
  parcel-centroids.csv \
  parcel-area-details \
  -sql "$(cat <<EOF
select
  geoparceli as gpin,
  y(st_centroid(st_union(geometry))) as lat,
  x(st_centroid(st_union(geometry))) as lon
from parcel_area_details
group by geoparceli
EOF
)"
python walkscore.py
bq load --autodetect --replace whatthecarp:cville_eda_raw.walkscore parcel-walk-scores.csv

# https://www.epa.gov/smartgrowth/smart-location-mapping
curl -O ftp://newftp.epa.gov/EPADataCommons/OP/Natl_WI_SHP.zip
unzip Natl_WI_SHP.zip
geojsonify Natl_WI_SHP natl_wi_shp.csv "select *, asgeojson(geometry) from natl_wi where geoid10 like '51540%'"
bq load --autodetect --replace whatthecarp:cville_eda_raw.epa_nwi natl_wi_shp.csv

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.geopin_to_nwi` as
select
  gpin.gpin,
  nwi.natwalkind
from `whatthecarp.cville_eda_derived.geopin_to_block` gpin
join `whatthecarp.cville_eda_raw.epa_nwi` nwi on cast(floor(gpin.geoid10 / 1000) as int64) = nwi.geoid10'
