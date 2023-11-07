--Part 3a
with nb as
(
select 
f.listing_neighbourhood,f.listing_neighbourhood_lga_code ,
SUM((30 - availability_30)*price) as Total_revenues
from "postgres"."warehouse"."facts_listings" as f
left join 
(SELECT
listing_neighbourhood,
MAX(scraped_date) AS newest_date
FROM "postgres"."warehouse"."facts_listings"
GROUP BY listing_neighbourhood) as d
on f.listing_neighbourhood =d.listing_neighbourhood 
where f.scraped_date>=d.newest_date - INTERVAL '12 months'
group by f.listing_neighbourhood,f.listing_neighbourhood_lga_code 
),
best_wost as (
SELECT * FROM nb
WHERE Total_revenues = (SELECT MIN(Total_revenues) FROM nb)
UNION ALL
SELECT * FROM nb
WHERE Total_revenues = (SELECT MAX(Total_revenues) FROM nb)
)
select b.listing_neighbourhood,b.Total_revenues,
((g01.age_0_4_yr_p+g01.age_5_14_yr_p+g01.age_15_19_yr_p+g01.age_20_24_yr_p+g01.age_25_34_yr_p)::numeric /nullif(g01.tot_p_p::numeric,0))*100 as percentage_0_34,
((g01.age_35_44_yr_p+g01.age_45_54_yr_p+g01.age_55_64_yr_p)::numeric /nullif(g01.tot_p_p::numeric,0))*100 as percentage_35_64,
((g01.age_65_74_yr_p+g01.age_75_84_yr_p+g01.age_85ov_p)::numeric /nullif(g01.tot_p_p::numeric,0))*100 as percentage_over_65

from best_wost b
left join "postgres"."warehouse"."dim_g01" as g01
on b.listing_neighbourhood_lga_code= g01.lga_code
;

--Task 3b
with top5 as (select  
f2.listing_neighbourhood,f2.property_description ,f2.room_description ,f2.accommodates,max_stay.Total_revenue
FROM 
(select listing_neighbourhood,property_description ,room_description ,accommodates,30 - availability_30 as stay
from "postgres"."warehouse"."facts_listings" )as f2
inner join 
(SELECT
    f.listing_neighbourhood,max(30 - availability_30) as stay,top5.Total_revenue
FROM "postgres"."warehouse"."facts_listings" as f
inner join 
(SELECT
    listing_neighbourhood,
	SUM((30 - availability_30)*price) as Total_revenue
FROM "postgres"."warehouse"."facts_listings"
GROUP BY listing_neighbourhood 
order by Total_revenue desc
limit 5) top5
on f.listing_neighbourhood =top5.listing_neighbourhood
group by f.listing_neighbourhood,top5.Total_revenue
order by top5.Total_revenue desc) as max_stay
on f2.listing_neighbourhood = max_stay.listing_neighbourhood
and f2.stay = max_stay.stay
),

top5_fre as(
SELECT
        listing_neighbourhood,
        property_description ,room_description ,accommodates,Total_revenue,
        COUNT(*) AS frequency
    FROM top5
GROUP BY listing_neighbourhood, property_description ,room_description ,accommodates,Total_revenue
ORDER BY listing_neighbourhood, frequency desc
)
SELECT DISTINCT ON (listing_neighbourhood)
listing_neighbourhood,property_description ,room_description ,accommodates,Total_revenue
FROM top5_fre
ORDER BY listing_neighbourhood, frequency,Total_revenue DESC
;

--Task3c
with host_same_lga as (
select 
a.host_id,a.num_lists,
coalesce(b.num_same_lga,0) as num_same_lga,
(coalesce(b.num_same_lga,0)::numeric/a.num_lists::numeric)*100 as per_same_lga
from 
(select distinct host_id, count(distinct listing_id) as num_lists
FROM "postgres"."warehouse"."facts_listings"
group by host_id 
having count(distinct listing_id) >1) as a
left join
(SELECT
distinct host_id,
COUNT(distinct listing_id) AS num_same_lga
FROM "postgres"."warehouse"."facts_listings"
WHERE host_neighbourhood_lga = listing_neighbourhood
GROUP BY host_id) as b
on a.host_id=b.host_id)

select count(host_id) as num_host,
sum(num_lists) as total_list,
sum(num_same_lga) as total_list_host_lga,
avg(per_same_lga) as per_same_lga 
from host_same_lga;

--Task3d
with onelist as(
select distinct host_id,host_neighbourhood_lga_code, count(distinct listing_id) as num_lists
FROM "postgres"."warehouse"."facts_listings"
group by host_id,host_neighbourhood_lga_code 
having count(distinct listing_id) =1),

newest_date as (
SELECT
host_id ,
MAX(scraped_date) AS newest_date
FROM "postgres"."warehouse"."facts_listings"
GROUP BY host_id
),

total_rev as (
select 
distinct f.host_id ,
SUM((30 - availability_30)*price) as Total_revenues
from "postgres"."warehouse"."facts_listings" as f
left join newest_date d
on f.host_id =d.host_id 
where f.scraped_date>=d.newest_date - INTERVAL '12 months'
group by f.host_id),

annual as (
select distinct o.host_id,o.host_neighbourhood_lga_code,t.Total_revenues,12*g02.median_mortgage_repay_monthly as median_mortgage_repay_annual  
from onelist o
left join total_rev t
on o.host_id=t.host_id
left join "postgres"."warehouse"."dim_g02" g02
on o.host_neighbourhood_lga_code=g02.lga_code)

select count(distinct host_id) as num_host,
    (COUNT(*) FILTER (WHERE Total_revenues > median_mortgage_repay_annual) * 100.0) / COUNT(*) AS percentage_covered
from annual
where median_mortgage_repay_annual is not null;

