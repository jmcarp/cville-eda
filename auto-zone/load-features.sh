#!/bin/bash

set -euo pipefail

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.autozone_features` as
select
  gpin.gpin,
  cat.distance as cat_distance,
  uts.distance as uts_distance,
  parks.distance as parks_distance,
  schools.distance as schools_distance,
  roads.distance as roads_distance,
  nwi.natwalkind,
  walkscore.walkscore,
  walkscore.transitscore,
  walkscore.bikescore,
  values.landvalue,
  values.sqm,
  values.landvaluepersqm,
  values.landvaluepersqmrank,
  vbg.landvaluepersqm as landvaluepersqm_parcel,
  percent_rank() over (order by vbg.landvaluepersqm) as landvaluepersqmrank_parcel,
  acs_blockgroup.prop_white,
  acs_blockgroup.prop_black,
  acs_blockgroup.prop_owner_occupied,
  acs_blockgroup.income,
  acs_blockgroup.prop_post_hs_degree,
  acs_tract.prop_families_in_poverty,
from `whatthecarp.cville_eda_derived.geopin` gpin
left join `whatthecarp.cville_eda_derived.geopin_to_cat` cat on gpin.gpin = cat.gpin
left join `whatthecarp.cville_eda_derived.geopin_to_uts` uts on gpin.gpin = uts.gpin
left join `whatthecarp.cville_eda_derived.geopin_to_park` parks on gpin.gpin = parks.gpin
left join `whatthecarp.cville_eda_derived.geopin_to_school` schools on gpin.gpin = schools.gpin
left join `whatthecarp.cville_eda_derived.geopin_to_roads` roads on gpin.gpin = roads.gpin
left join `whatthecarp.cville_eda_derived.geopin_to_nwi` nwi on gpin.gpin = nwi.gpin
left join `whatthecarp.cville_eda_raw.walkscore` walkscore on gpin.gpin = walkscore.gpin
left join `whatthecarp.cville_eda_derived.geopin_to_block` gpin_to_block on gpin.gpin = gpin_to_block.gpin
left join `whatthecarp.cville_eda_derived.value_by_block` values on gpin_to_block.geoid10 = values.geoid10
left join `whatthecarp.cville_eda_derived.acs_blockgroup` acs_blockgroup on cast(floor(gpin_to_block.geoid10 / 1000) as int64) = cast(acs_blockgroup.geoid10 as int64)
left join `whatthecarp.cville_eda_derived.acs_tract` acs_tract on cast(floor(gpin_to_block.geoid10 / 10000) as int64) = cast(acs_tract.geoid10 as int64)
left join `whatthecarp.cville_eda_derived.value_by_geopin` vbg on gpin.gpin = vbg.gpin
where gpin.gpin not in (
  select
    gpin
  from `whatthecarp.cville_eda_derived.school_parcels`
  union all
  select
    gpin
  from `whatthecarp.cville_eda_derived.park_parcels`
)'

bq extract whatthecarp:cville_eda_derived.autozone_features gs://whatthecarp-scratch/autozone_features.csv
gsutil cp gs://whatthecarp-scratch/autozone_features.csv .
