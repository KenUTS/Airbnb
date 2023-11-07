{{
    config(
        unique_key='surrogate_key'
    )
}}


with check_dimensions as
(select
	surrogate_key,
    LISTING_ID,
    SCRAPE_ID,
    SCRAPED_DATE,
    LISTING_NEIGHBOURHOOD,
    case when host_id in (select distinct host_id from {{ ref('host_stg') }}) then host_id else 0 end as host_id,
    case when room_id in (select distinct room_id from {{ ref('room_stg') }}) then room_id else 0 end as room_id,
    case when property_id in (select distinct property_id from {{ ref('property_stg') }}) then property_id else 0 end as property_id,
    ACCOMMODATES,
    PRICE,
    HAS_AVAILABILITY,
    AVAILABILITY_30,
    NUMBER_OF_REVIEWS,
    REVIEW_SCORES_RATING,
    REVIEW_SCORES_ACCURACY,
    REVIEW_SCORES_CLEANLINESS,
    REVIEW_SCORES_CHECKIN,
    REVIEW_SCORES_COMMUNICATION,
    REVIEW_SCORES_VALUE
from {{ ref('listing_stg') }})
,

listing as (
select
	a.surrogate_key,
    a.LISTING_ID,
    a.SCRAPE_ID,
    a.SCRAPED_DATE,
    a.LISTING_NEIGHBOURHOOD,
    e.lga_code as LISTING_NEIGHBOURHOOD_lga_code,
    b.host_id,
    b.host_name,
    b.host_since,
    b.host_is_superhost,
    b.host_neighbourhood,
    c.room_id,
    c.room_description,
    d.property_id,
    d.property_description,
    a.ACCOMMODATES,
    a.PRICE,
    a.HAS_AVAILABILITY,
    a.AVAILABILITY_30,
    a.NUMBER_OF_REVIEWS,
    a.REVIEW_SCORES_RATING,
    a.REVIEW_SCORES_ACCURACY,
    a.REVIEW_SCORES_CLEANLINESS,
    a.REVIEW_SCORES_CHECKIN,
    a.REVIEW_SCORES_COMMUNICATION,
    a.REVIEW_SCORES_VALUE
from check_dimensions a
left join staging.host_stg b  on a.host_id = b.host_id and a.SCRAPED_DATE::date >= b.dbt_valid_from and a.SCRAPED_DATE::date < coalesce(b.dbt_valid_to, '9999-12-31'::date)
left join staging.room_stg c  on a.room_id = c.room_id and a.SCRAPED_DATE::date >= c.dbt_valid_from and a.SCRAPED_DATE::date < coalesce(c.dbt_valid_to, '9999-12-31'::date)
left join staging.property_stg d  on a.property_id = d.property_id and a.SCRAPED_DATE::date >= d.dbt_valid_from and a.SCRAPED_DATE::date < coalesce(d.dbt_valid_to, '9999-12-31'::date)
left join staging.lga_stg e  on a.LISTING_NEIGHBOURHOOD = e.lga_name )
,
listing_suburb as (
select
	l.surrogate_key,
    l.LISTING_ID,
    l.SCRAPE_ID,
    l.SCRAPED_DATE,
    l.LISTING_NEIGHBOURHOOD,
    l.LISTING_NEIGHBOURHOOD_lga_code,
    l.host_id,
    l.host_name,
    l.host_since,
    l.host_is_superhost,
    l.host_neighbourhood,
    f.lga_name as host_neighbourhood_lga,
    l.room_id,
    l.room_description,
    l.property_id,
    l.property_description,
    l.ACCOMMODATES,
    l.PRICE,
    l.HAS_AVAILABILITY,
    l.AVAILABILITY_30,
    l.NUMBER_OF_REVIEWS,
    l.REVIEW_SCORES_RATING,
    l.REVIEW_SCORES_ACCURACY,
    l.REVIEW_SCORES_CLEANLINESS,
    l.REVIEW_SCORES_CHECKIN,
    l.REVIEW_SCORES_COMMUNICATION,
    l.REVIEW_SCORES_VALUE
from listing l
left join staging.suburb_stg f  on l.host_neighbourhood = f.suburb_name )

select
	l.surrogate_key,
    l.LISTING_ID,
    l.SCRAPE_ID,
    l.SCRAPED_DATE,
    l.LISTING_NEIGHBOURHOOD,
    l.LISTING_NEIGHBOURHOOD_lga_code,
    l.host_id,
    l.host_name,
    l.host_since,
    l.host_is_superhost,
    l.host_neighbourhood,
    l.host_neighbourhood_lga,
    g.lga_code as host_neighbourhood_lga_code,
    l.room_description,
    l.property_description,
    l.ACCOMMODATES,
    l.PRICE,
    l.HAS_AVAILABILITY,
    l.AVAILABILITY_30,
    l.NUMBER_OF_REVIEWS,
    l.REVIEW_SCORES_RATING,
    l.REVIEW_SCORES_ACCURACY,
    l.REVIEW_SCORES_CLEANLINESS,
    l.REVIEW_SCORES_CHECKIN,
    l.REVIEW_SCORES_COMMUNICATION,
    l.REVIEW_SCORES_VALUE
from listing_suburb l
left join staging.lga_stg g  on l.host_neighbourhood_lga = g.lga_name 