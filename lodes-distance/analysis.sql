-- https://community.looker.com/lookml-5/analytic-block-weighted-medians-in-bigquery-with-udfs-4155
CREATE TEMP FUNCTION _pairs_sum_float(a ARRAY<STRUCT<num FLOAT64, weight FLOAT64>>)
RETURNS ARRAY<STRUCT<num FLOAT64, weight FLOAT64>> AS ((
  SELECT
    ARRAY_AGG(STRUCT(num, weight))
  FROM (
    SELECT
      num,
      SUM(weight) as weight
    FROM UNNEST(a)
    GROUP BY 1
    ORDER BY 2 DESC
  )
));

CREATE TEMP FUNCTION MEDIAN_WEIGHTED(a_nums ARRAY<STRUCT<num FLOAT64, weight FLOAT64>>)
RETURNS FLOAT64 AS ((
  SELECT
    num
  FROM (
    SELECT
      MAX(cumulative_weight) OVER() max_weight,
      cumulative_weight,
      num
    FROM (
      SELECT
        SUM(weight) OVER (ORDER BY num) as cumulative_weight,
        weight,
        num
      FROM UNNEST(_pairs_sum_float(a_nums)) a
      ORDER BY num
    )
  )
  WHERE cumulative_weight > max_weight / 2
  ORDER BY num
  LIMIT 1
));

create or replace table demsstaff.sbx_carpj.lodes_va_distance as
select
  lodes.*,
  st_distance(
    st_geogfromgeojson(cw.centroid),
    st_geogfromgeojson(ch.centroid)
  ) as distance,
from lodes_va lodes
join centroids_va cw on lodes.w_geocode = cw.geoid10
join centroids_va ch on lodes.h_geocode = ch.geoid10

create or replace table `demsstaff.sbx_carpj.lodes_va_distance_w_tract` as
select
  substr(cast(w_geocode as string), 1, 11) as w_tract,
  median_weighted(array_agg(struct(distance, cast(S000 as float64)))) as median_distance,
  sum(S000) as S000,
  sum(SA01) as SA01,
  sum(SA02) as SA02,
  sum(SA03) as SA03,
  sum(SE01) as SE01,
  sum(SE02) as SE02,
  sum(SE03) as SE03,
  sum(SI01) as SI01,
  sum(SI02) as SI02,
  sum(SI03) as SI03
from demsstaff.sbx_carpj.lodes_va_distance
group by 1

create or replace table `demsstaff.sbx_carpj.lodes_va_distance_h_tract` as
select
  substr(cast(h_geocode as string), 1, 11) as h_tract,
  median_weighted(array_agg(struct(distance, cast(S000 as float64)))) as median_distance,
  sum(S000) as S000,
  sum(SA01) as SA01,
  sum(SA02) as SA02,
  sum(SA03) as SA03,
  sum(SE01) as SE01,
  sum(SE02) as SE02,
  sum(SE03) as SE03,
  sum(SI01) as SI01,
  sum(SI02) as SI02,
  sum(SI03) as SI03
from demsstaff.sbx_carpj.lodes_va_distance
group by 1
