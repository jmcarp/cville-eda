#!/bin/bash

set -euo pipefail

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.autozone_features` as
select
  details.parcelnumb as parcelnumber,
  cat.distance as cat_distance,
  parks.distance as parks_distance,
  roads_local.distance as roads_local_distance,
  roads_primary.distance as roads_primary_distance,
  roads_secondary.distance as roads_secondary_distance,
  roads_tertiary.distance as roads_tertiary_distance,
  values.landvalueperacre,
from `whatthecarp.cville_eda_raw.parcel_area_details` details
left join `whatthecarp.cville_eda_derived.parcel_to_cat` cat on details.parcelnumb = cat.parcelnumber
left join `whatthecarp.cville_eda_derived.parcel_to_park` parks on details.parcelnumb = parks.parcelnumber
left join `whatthecarp.cville_eda_derived.roads_local_distance` roads_local on details.parcelnumb = roads_local.parcelnumber
left join `whatthecarp.cville_eda_derived.roads_primary_distance` roads_primary on details.parcelnumb = roads_primary.parcelnumber
left join `whatthecarp.cville_eda_derived.roads_secondary_distance` roads_secondary on details.parcelnumb = roads_secondary.parcelnumber
left join `whatthecarp.cville_eda_derived.roads_tertiary_distance` roads_tertiary on details.parcelnumb = roads_tertiary.parcelnumber
left join `whatthecarp.cville_eda_derived.parcel_to_block` parcel_to_block on details.parcelnumb = parcel_to_block.parcelnumber
left join `whatthecarp.cville_eda_derived.value_by_block` values on parcel_to_block.geoid10 = values.geoid10'

bq extract whatthecarp:cville_eda_derived.autozone_features gs://whatthecarp-scratch/autozone_features.csv
