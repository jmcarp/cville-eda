#!/bin/bash

set -euo pipefail

python load-census.py

bq load --autodetect --replace whatthecarp:cville_eda_raw.sf1 sf1-export.csv
bq load --autodetect --replace whatthecarp:cville_eda_raw.acs_blockgroup acs-blockgroup-export.csv
bq load --autodetect --replace whatthecarp:cville_eda_raw.acs_blockgroup_by_year acs-blockgroup-by-year-export.csv
bq load --autodetect --replace whatthecarp:cville_eda_raw.acs_tract acs-tract-export.csv

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.sf1` as
select
  sf1.*,
  concat(format("%02d", state), format("%03d", county), format("%06d", tract), format("%04d", block)) as geoid10,
  safe_divide(P003002, P003001) as prop_white,
  safe_divide(P003003, P003001) as prop_black,
  safe_divide(P005003, P005001) as prop_white_not_hispanic,
  safe_divide(P005004 + P005010, P005001) as prop_black_or_hispanic,
from `whatthecarp.cville_eda_raw.sf1` sf1
where concat(format("%02d", state), format("%03d", county), format("%06d", tract), format("%01d", cast(floor(block / 1000) as int64))) not in (
  select
    blockgroup
  from `whatthecarp.cville_eda_raw.student_blockgroups`
)'
bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.acs_blockgroup` as
select
  *,
  concat(format("%02d", state), format("%03d", county), format("%06d", tract), format("%01d", block_group)) as geoid10,
  safe_divide(B02001_002E, B02001_001E) as prop_white,
  safe_divide(B02001_003E, B02001_001E) as prop_black,
  safe_divide(B25003_002E, B25003_001E) as prop_owner_occupied,
  if(B19013_001E >= 0, B19013_001E, null) as income,
  safe_divide(B15003_021E + B15003_022E + B15003_023E + B15003_024E + B15003_025E, B15003_001E) as prop_post_hs_degree,
from `whatthecarp.cville_eda_raw.acs_blockgroup`
where concat(format("%02d", state), format("%03d", county), format("%06d", tract), format("%01d", block_group)) not in (
  select
    blockgroup
  from `whatthecarp.cville_eda_raw.student_blockgroups`
)'
bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.acs_tract` as
select
  *,
  concat(format("%02d", state), format("%03d", county), format("%06d", tract)) as geoid10,
  safe_divide(B14001_008E, B14001_001E) as prop_college_students,
  if(safe_divide(B14001_008E, B14001_001E) < 0.5, safe_divide(B17012_002E, B17012_001E), null) as prop_families_in_poverty,
from `whatthecarp.cville_eda_raw.acs_tract`'
