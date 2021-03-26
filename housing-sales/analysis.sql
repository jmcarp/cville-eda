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
  from whatthecarp.cville_eda.real_estate_sales sales
  join (
    select
      *,
      rank() over (partition by parcelnumber order by objectid) as rank
    from whatthecarp.cville_eda.parcel_area_details
  ) details on sales.parcelnumber = details.parcelnumber
  where saleamount != 0
  and rank = 1
)
where zoning in ('R-1', 'R-2', 'R-3')
and saleyear >= 2001
order by saleyear desc, zoning
