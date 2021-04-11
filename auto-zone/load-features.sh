#!/bin/bash

set -euo pipefail

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.autozone_features` as
select
  gpin.gpin,
  cat.distance as cat_distance,
  parks.distance as parks_distance,
  roads.distance as roads_distance,
  nwi.natwalkind,
  (nwi.natwalkind - avg(nwi.natwalkind) over ()) / (max(nwi.natwalkind) over () - min(nwi.natwalkind) over ()) + 0.5 as natwalkind_norm,
  walkscore.walkscore / 100 as walkscore,
  walkscore.transitscore / 100 as transitscore,
  walkscore.bikescore / 100 as bikescore,
  values.landvaluepersqm,
  (values.landvaluepersqm - avg(values.landvaluepersqm) over ()) / (max(values.landvaluepersqm) over () - min(values.landvaluepersqm) over ()) + 0.5 as landvaluepersqm_norm,
from `whatthecarp.cville_eda_derived.geopin` gpin
left join `whatthecarp.cville_eda_derived.geopin_to_cat` cat on gpin.gpin = cat.gpin
left join `whatthecarp.cville_eda_derived.geopin_to_park` parks on gpin.gpin = parks.gpin
left join `whatthecarp.cville_eda_derived.geopin_to_roads` roads on gpin.gpin = roads.gpin
left join `whatthecarp.cville_eda_derived.geopin_to_nwi` nwi on gpin.gpin = nwi.gpin
left join `whatthecarp.cville_eda_raw.walkscore` walkscore on gpin.gpin = walkscore.gpin
left join `whatthecarp.cville_eda_derived.geopin_to_block` gpin_to_block on gpin.gpin = gpin_to_block.gpin
left join `whatthecarp.cville_eda_derived.value_by_block` values on gpin_to_block.geoid10 = values.geoid10'

bq extract whatthecarp:cville_eda_derived.autozone_features gs://whatthecarp-scratch/autozone_features.csv
gsutil cp gs://whatthecarp-scratch/autozone_features.csv .
