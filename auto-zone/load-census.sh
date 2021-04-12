#!/bin/bash

set -euo pipefail

python load-sf1.py

bq load --autodetect --replace whatthecarp:cville_eda_raw.sf1_race sf1-race.csv
bq load --autodetect --replace whatthecarp:cville_eda_raw.acs_race acs-race.csv

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.sf1_race` as
select 
  *,
  concat(format("%02d", state), format("%03d", county), format("%06d", tract), format("%04d", block)) as geoid10,
  safe_divide(P003002, P003001) as prop_white,
  safe_divide(P003003, P003001) as prop_black,
  safe_divide(P005003, P005001) as prop_white_not_hispanic,
  safe_divide(P005004 + P005010, P005001) as prop_black_or_hispanic,
from `whatthecarp.cville_eda_raw.sf1_race`'
bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.acs_race` as
select 
  *,
  concat(format("%02d", state), format("%03d", county), format("%06d", tract), format("%01d", block_group)) as geoid10,
  safe_divide(B02001_002E, B02001_001E) as prop_white,
  safe_divide(B02001_003E, B02001_001E) as prop_black,
from `whatthecarp.cville_eda_raw.acs_race`'
