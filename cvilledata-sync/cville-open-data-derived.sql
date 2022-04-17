-- Aggregate parcels by geopin
create or replace table `cvilledata.cville_open_data_derived.geopin` as
select
  geoparceli as gpin,
  st_union_agg(st_geogfromgeojson(geometry)) as geometry
from `cvilledata.cville_open_data.parcel_area_details`
group by geoparceli
;

-- Map geopin to planning neighborhood
create or replace table `cvilledata.cville_open_data_derived.geopin_to_planning_neighborhood` as
select
  * except (rank)
from (
  select
    gpin.gpin,
    neighborhood.name,
    st_distance(gpin.geometry, st_geogfromgeojson(neighborhood.geometry)) as distance,
    row_number() over (partition by gpin.gpin order by st_distance(gpin.geometry, st_geogfromgeojson(neighborhood.geometry)) asc) as rank
  from `cvilledata.cville_open_data_derived.geopin` gpin
  cross join `cvilledata.cville_open_data.planning_neighborhood_area` neighborhood
)
where rank = 1
;

-- Map geopin to assessment neighborhood
create or replace table `cvilledata.cville_open_data_derived.geopin_to_assessment_neighborhood` as
select
  * except (rank)
from (
  select
    gpin.gpin,
    neighborhood.neighhood as name,
    neighborhood.neighcode as code,
    st_distance(gpin.geometry, st_geogfromgeojson(neighborhood.geometry)) as distance,
    row_number() over (partition by gpin.gpin order by st_distance(gpin.geometry, st_geogfromgeojson(neighborhood.geometry)) asc) as rank
  from `cvilledata.cville_open_data_derived.geopin` gpin
  cross join `cvilledata.cville_open_data.assessment_neighborhoods` neighborhood
)
where rank = 1
;

-- Map geopin to 2010 census tract
create or replace table `cvilledata.cville_open_data_derived.geopin_to_tract_2010` as
select
  * except (rank)
from (
  select
    gpin.gpin,
    tracts.countyfp10,
    tracts.tractce10,
    tracts.geoid10,
    row_number() over (partition by gpin.gpin order by st_area(st_intersection(gpin.geometry, st_geogfromgeojson(tracts.geometry))) desc) as rank
  from `cvilledata.cville_open_data_derived.geopin` gpin
  cross join `cvilledata.census.census_tracts_2010_51` tracts
  where tracts.countyfp10 = 540
)
where rank = 1
;

-- Map geopin to 2010 census block
create or replace table `cvilledata.cville_open_data_derived.geopin_to_block_2010` as
select
  * except (rank)
from (
  select
    gpin.gpin,
    blocks.countyfp10,
    blocks.tractce10,
    blocks.blockce10,
    blocks.geoid10,
    row_number() over (partition by gpin.gpin order by st_area(st_intersection(gpin.geometry, st_geogfromgeojson(blocks.geometry))) desc) as rank
  from `cvilledata.cville_open_data_derived.geopin` gpin
  join `cvilledata.cville_open_data_derived.geopin_to_tract_2010` tracts on gpin.gpin = tracts.gpin
  join `cvilledata.census.census_blocks_2010_51` blocks on tracts.tractce10 = blocks.tractce10
  where blocks.countyfp10 = 540
)
where rank = 1
;

-- Map geopin to 2020 census tract
create or replace table `cvilledata.cville_open_data_derived.geopin_to_tract_2020` as
select
  * except (rank)
from (
  select
    gpin.gpin,
    tracts.countyfp,
    tracts.tractce,
    tracts.geoid,
    row_number() over (partition by gpin.gpin order by st_area(st_intersection(gpin.geometry, st_geogfromgeojson(tracts.geometry))) desc) as rank
  from `cvilledata.cville_open_data_derived.geopin` gpin
  cross join `cvilledata.census.census_tracts_2020_51` tracts
  where tracts.countyfp = 540
)
where rank = 1
;

-- Map geopin to 2020 census block
create or replace table `cvilledata.cville_open_data_derived.geopin_to_block_2020` as
select
  * except (rank)
from (
  select
    gpin.gpin,
    blocks.countyfp20,
    blocks.tractce20,
    blocks.blockce20,
    blocks.geoid20,
    row_number() over (partition by gpin.gpin order by st_area(st_intersection(gpin.geometry, st_geogfromgeojson(blocks.geometry))) desc) as rank
  from `cvilledata.cville_open_data_derived.geopin` gpin
  join `cvilledata.cville_open_data_derived.geopin_to_tract_2020` tracts on gpin.gpin = tracts.gpin
  join `cvilledata.census.census_blocks_2020_51` blocks on tracts.tractce = blocks.tractce20
  where blocks.countyfp20 = 540
)
where rank = 1
;

-- Map geopin to sensitive area classification
-- See https://www.charlottesville.gov/DocumentCenter/View/7073/Comprehensive-Plan-Document---2021-1115-Final,
-- page 27, figure 7
create or replace table `cvilledata.cville_open_data_derived.geopin_to_sensitive_area` as
select
  gpin,
  floor(geoid10 / 1000) in (
    515400002012,
    515400002021,
    515400004011,
    515400004023,
    515400005011,
    515400005013,
    515400008004
  ) as is_sensitive_area,
from `cvilledata.cville_open_data_derived.geopin_to_block`
;

-- Map structure to geopin
create or replace table `cvilledata.cville_open_data_derived.structure_to_geopin` as
select
  structures.bin,
  geopin.gpin,
from `cvilledata.cville_open_data.existing_structure_area` structures
join `cvilledata.cville_open_data_derived.geopin` geopin on
  st_intersects(st_geogfromgeojson(structures.geometry, make_valid => true), geopin.geometry)
;
