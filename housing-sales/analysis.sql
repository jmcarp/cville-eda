-- Calculate median sales price by year by residential zoning
create or replace table whatthecarp.cville_eda_derived.sales_by_year as
select distinct
  saleyear,
  zoning,
  saleamount,
  count
from (
  select
    extract(year from saledate) as saleyear,
    details.zoning,
    percentile_cont(saleamount, 0.5) over (partition by extract(year from saledate), details.zoning) as saleamount,
    count(saleamount) over (partition by extract(year from saledate), details.zoning) as count
  from whatthecarp.cville_eda_raw.real_estate_sales sales
  join (
    select
      *,
      rank() over (partition by parcelnumber order by objectid) as rank
    from whatthecarp.cville_eda_raw.parcel_area_details
  ) details on sales.parcelnumber = details.parcelnumber
  where saleamount != 0
  and rank = 1
)
where zoning in ('R-1', 'R-2', 'R-3')
and saleyear >= 2001
;

create or replace table whatthecarp.cville_eda_derived.sales_by_acreage as
with base as(
  select
    *,
    rank() over (partition by parcelnumber order by RecordID_Int) as rank
  from `whatthecarp.cville_eda_raw.real_estate_base`
), details as (
  select
    *,
    rank() over (partition by parcelnumber order by objectid) as rank
  from whatthecarp.cville_eda_raw.parcel_area_details
)
select
  base.parcelnumber,
  base.acreage,
  extract(year from sales.saledate) as saleyear,
  sales.saleamount,
from whatthecarp.cville_eda_raw.real_estate_sales sales
join base using (parcelnumber)
join details using (parcelnumber)
where saleamount != 0
and base.rank = 1
and details.rank = 1
and details.zoning in ('R-1', 'R-2', 'R-3')
and extract(year from saledate) > 2001
;
