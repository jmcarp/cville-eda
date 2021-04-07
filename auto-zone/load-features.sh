#!/bin/bash

set -euo pipefail

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.autozone_features` as
select
  gpin.gpin,
  cat.distance as cat_distance,
  parks.distance as parks_distance,
  roads_local.distance as roads_local_distance,
  roads_primary.distance as roads_primary_distance,
  roads_secondary.distance as roads_secondary_distance,
  roads_tertiary.distance as roads_tertiary_distance,
  values.landvaluepersqm,
from `whatthecarp.cville_eda_derived.geopin` gpin
left join `whatthecarp.cville_eda_derived.geopin_to_cat` cat on gpin.gpin = cat.gpin
left join `whatthecarp.cville_eda_derived.geopin_to_park` parks on gpin.gpin = parks.gpin
left join `whatthecarp.cville_eda_derived.roads_local_distance` roads_local on gpin.gpin = roads_local.gpin
left join `whatthecarp.cville_eda_derived.roads_primary_distance` roads_primary on gpin.gpin = roads_primary.gpin
left join `whatthecarp.cville_eda_derived.roads_secondary_distance` roads_secondary on gpin.gpin = roads_secondary.gpin
left join `whatthecarp.cville_eda_derived.roads_tertiary_distance` roads_tertiary on gpin.gpin = roads_tertiary.gpin
left join `whatthecarp.cville_eda_derived.geopin_to_block` gpin_to_block on gpin.gpin = gpin_to_block.gpin
left join `whatthecarp.cville_eda_derived.value_by_block` values on gpin_to_block.geoid10 = values.geoid10'

bq extract whatthecarp:cville_eda_derived.autozone_features gs://whatthecarp-scratch/autozone_features.csv
gsutil cp gs://whatthecarp-scratch/autozone_features.csv .
