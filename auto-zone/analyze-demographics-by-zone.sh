#!/bin/bash

set -euo pipefail

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.parcel_to_address` as
select
  parcelnumb as parcelnumber,
  address,
  geo_mat_ma as masteraddressid,
from (
  select
    details.*,
    points.*,
    row_number() over (partition by points.objectid order by st_distance(points.geography, st_geogfromgeojson(details.geometry))) as rank,
  from `whatthecarp.cville_eda_raw.parcel_area_details` details
  cross join `whatthecarp.cville_eda_derived.master_address_points` points
  where st_dwithin(points.geography, st_geogfromgeojson(details.geometry), 15)
)
where rank = 1'

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.parcel_to_address_geocode` as
select
  parcelnumb as parcelnumber,
  address,
  address_id,
from (
  select
    details.*,
    geocoded.*,
    row_number() over (partition by geocoded.address_id order by st_distance(st_geogpoint(geocoded.longitude, geocoded.latitude), st_geogfromgeojson(details.geometry))) as rank,
  from `whatthecarp.cville_eda_raw.parcel_area_details` details
  cross join `whatthecarp.cville_eda_raw.master_addresses_geocoded` geocoded
  where st_dwithin(st_geogpoint(geocoded.longitude, geocoded.latitude), st_geogfromgeojson(details.geometry), 15)
)
where rank = 1'

bq query --nouse_legacy_sql \
'create or replace function whatthecarp.cville_eda_derived.standardize_zone(zone string) returns string as (
case zone
  when "R-1UH" then "R-1U"
  when "R-1H" then "R-1"
  when "B-1H" then "B-1"
  when "UHDH" then "UHD"
  when "UMDH" then "UMD"
  when "R-2C" then "R-2"
  when "DNH" then "DN"
  when "R-1SC" then "R-1S"
  when "WSH" then "WS"
  when "R-1SH" then "R-1S"
  when "R-3H" then "R-3"
  when "R-1C" then "R-1"
  when "MLTPC" then "MLTP"
  when "R-2UH" then "R-2U"
  when "R-1SUH" then "R-1SU"
  when "DNC" then "DN"
  when "CDH" then "CD"
  when "B-3H" then "B-3"
  when "DH" then "D"
  when "SSH" then "SS"
  when "WMEH" then "WME"
  when "R-2H" then "R-2"
  when "CCH" then "CC"
  when "WMWH" then "WMW"
  when "B-1C" then "B-1"
  when "CHH" then "CH"
  when "R-1SHC" then "R-1S"
  when "PUDH" then "PUD"
  when "WMSH" then "WMS"
  when "HSC" then "HS"
  when "MLTPH" then "MLTP"
  when "WSDH" then "WSD"
  when "B-2H" then "B-2"
  when "MRH" then "MR"
  when "ICH" then "IC"
  when "WMNH" then "WMN"
  when "DEH" then "DE"
  when "NCCH" then "NCC"
  else zone
end);'

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.sf1_by_zoning` as
select
  whatthecarp.cville_eda_derived.standardize_zone(details.zoning) as zoning,
  avg(safe_divide(P003003, P003001)) as prop_black,
  avg(safe_divide(P003005, P003001)) as prop_asian,
  avg(safe_divide(P003002, P003001)) as prop_white,
  avg(1 - safe_divide(P003002 + P003003 + P003005, P003001)) as prop_other,
  count(details.zoning) as addresses,
from `whatthecarp.cville_eda_derived.parcel_to_address_geocode` p2a
join `whatthecarp.cville_eda_raw.parcel_area_details` details on p2a.parcelnumber = details.parcelnumb
join `whatthecarp.cville_eda_derived.geopin_to_block` g2b on details.geoparceli = g2b.gpin
join `whatthecarp.cville_eda_raw.sf1` sf1
  on cast(g2b.geoid10 as string) =
  concat(format("%02d", sf1.state), format("%03d", sf1.county), format("%06d", sf1.tract), format("%04d", sf1.block))
group by zoning'

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.acs_by_zoning` as
select
  year,
  whatthecarp.cville_eda_derived.standardize_zone(details.zoning) as zoning,
  avg(safe_divide(B02001_003E, B02001_001E)) as prop_black,
  avg(safe_divide(B02001_005E, B02001_001E)) as prop_asian,
  avg(safe_divide(B02001_002E, B02001_001E)) as prop_white,
  avg(1 - safe_divide(B02001_002E + B02001_003E + B02001_005E, B02001_001E)) as prop_other,
  avg(if(B19013_001E >= 0, B19013_001E, null)) as income,
  avg(safe_divide(B25032_003E + B25032_014E, B25032_001E)) as prop_sfd,
  count(details.zoning) as addresses,
from `whatthecarp.cville_eda_derived.parcel_to_address_geocode` p2a
join `whatthecarp.cville_eda_raw.parcel_area_details` details on p2a.parcelnumber = details.parcelnumb
join `whatthecarp.cville_eda_derived.geopin_to_block` g2b on details.geoparceli = g2b.gpin
join `whatthecarp.cville_eda_raw.acs_blockgroup_by_year` acs
  on cast(floor(g2b.geoid10 / 1000) as string) =
  concat(format("%02d", acs.state), format("%03d", acs.county), format("%06d", acs.tract), format("%01d", acs.block_group))
group by zoning, acs.year'

gsutil cp gs://whatthecarp-public/Draft_FLUM_May2021-web.zip .
unzip Draft_FLUM_May2021-web.zip -d draft-flum
geojsonify draft-flum draft-flum.csv
bq load --autodetect --replace whatthecarp:cville_eda_raw.draft_flum draft-flum.csv

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.sf1_by_flum` as
select
  flum.desig as designation,
  avg(safe_divide(P003003, P003001)) as prop_black,
  avg(safe_divide(P003005, P003001)) as prop_asian,
  avg(safe_divide(P003002, P003001)) as prop_white,
  avg(1 - safe_divide(P003002 + P003003 + P003005, P003001)) as prop_other,
  count(flum.desig) as addresses,
from `whatthecarp.cville_eda_derived.parcel_to_address_geocode` p2a
join `whatthecarp.cville_eda_raw.parcel_area_details` details on p2a.parcelnumber = details.parcelnumb
join `whatthecarp.cville_eda_raw.draft_flum` flum on details.geoparceli = flum.geoparceli
join `whatthecarp.cville_eda_derived.geopin_to_block` g2b on flum.geoparceli = g2b.gpin
join `whatthecarp.cville_eda_raw.sf1` sf1
  on cast(g2b.geoid10 as string) =
  concat(format("%02d", sf1.state), format("%03d", sf1.county), format("%06d", sf1.tract), format("%04d", sf1.block))
group by designation'

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.acs_by_flum` as
select
  year,
  flum.desig as designation,
  avg(safe_divide(B02001_003E, B02001_001E)) as prop_black,
  avg(safe_divide(B02001_005E, B02001_001E)) as prop_asian,
  avg(safe_divide(B02001_002E, B02001_001E)) as prop_white,
  avg(1 - safe_divide(B02001_002E + B02001_003E + B02001_005E, B02001_001E)) as prop_other,
  avg(if(B19013_001E >= 0, B19013_001E, null)) as income,
  avg(safe_divide(B25032_003E + B25032_014E, B25032_001E)) as prop_sfd,
  count(flum.desig) as addresses,
from `whatthecarp.cville_eda_derived.parcel_to_address_geocode` p2a
join `whatthecarp.cville_eda_raw.parcel_area_details` details on p2a.parcelnumber = details.parcelnumb
join `whatthecarp.cville_eda_raw.draft_flum` flum on details.geoparceli = flum.geoparceli
join `whatthecarp.cville_eda_derived.geopin_to_block` g2b on flum.geoparceli = g2b.gpin
join `whatthecarp.cville_eda_raw.acs_blockgroup_by_year` acs
  on cast(floor(g2b.geoid10 / 1000) as string) =
  concat(format("%02d", acs.state), format("%03d", acs.county), format("%06d", acs.tract), format("%01d", acs.block_group))
group by designation, acs.year'

bq query --nouse_legacy_sql \
'create or replace table `whatthecarp.cville_eda_derived.zone_to_flum` as
with curr as (
  select
    geoparceli,
    zoning
  from (
    select
      geoparceli,
      zoning,
      row_number() over (partition by geoparceli order by parcelnumb) as rank,
    from `whatthecarp.cville_eda_raw.parcel_area_details`
  )
  where rank = 1
)
select
  whatthecarp.cville_eda_derived.standardize_zone(curr.zoning) as zoning,
  flum.desig as designation,
  neighborhood.neighborhood_name as neighborhood,
  neighborhood.gpin,
from curr
join `whatthecarp.cville_eda_raw.draft_flum` flum on curr.geoparceli = flum.geoparceli
join `whatthecarp.cville_eda_derived.geopin_to_neighborhood` neighborhood on flum.geoparceli = neighborhood.gpin'

bq extract whatthecarp:cville_eda_derived.zone_to_flum gs://whatthecarp-scratch/zone_to_flum.csv
gsutil cp gs://whatthecarp-scratch/zone_to_flum.csv .
