-- Aggregate parcels by geopin
create or replace table `cvilledata.cville_open_data_derived.geopin` as
select
  geoparceli as gpin,
  st_union_agg(st_geogfromgeojson(geometry)) as geometry
from `cvilledata.cville_open_data.parcel_area_details`
group by geoparceli
;

-- Translate geopin to planning neighborhood
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

-- Translate geopin to assessment neighborhood
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
