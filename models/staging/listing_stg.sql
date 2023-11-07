{{
    config(
        unique_key='surrogate_key'
    )
}}

with

source  as (

    select * from "postgres"."raw"."listing"

),

transformed as (
    select
        ROW_NUMBER() OVER (ORDER BY listing_id, scraped_date) AS surrogate_key,
        cast(LISTING_ID	as Bigint),
        cast(SCRAPE_ID as Bigint),	
        TO_CHAR(CAST(SCRAPED_DATE AS DATE),'YYYY-MM-DD')::DATE as SCRAPED_DATE,
        case when LISTING_NEIGHBOURHOOD !='NaN' then UPPER(LISTING_NEIGHBOURHOOD) else 'unknown' end as LISTING_NEIGHBOURHOOD,
        cast(host_id as Bigint),
        dense_rank() over(order by room_type) as room_id,
        dense_rank() over(order by property_type) as property_id,
        CAST(CAST(ACCOMMODATES AS NUMERIC) AS INT),	
        CAST(CAST(PRICE AS NUMERIC) AS Bigint),	
        HAS_AVAILABILITY,	
        CAST(CAST(AVAILABILITY_30 AS NUMERIC) AS INT),
        CAST(CAST(NUMBER_OF_REVIEWS AS NUMERIC) AS Bigint),
        case when REVIEW_SCORES_RATING !='NaN' then CAST(CAST(REVIEW_SCORES_RATING AS NUMERIC) AS INT) else 0 end as REVIEW_SCORES_RATING,
        case when REVIEW_SCORES_ACCURACY !='NaN' then CAST(CAST(REVIEW_SCORES_ACCURACY AS NUMERIC) AS INT) else 0 end as REVIEW_SCORES_ACCURACY,
        case when REVIEW_SCORES_CLEANLINESS !='NaN' then CAST(CAST(REVIEW_SCORES_CLEANLINESS AS NUMERIC) AS INT) else 0 end as REVIEW_SCORES_CLEANLINESS,
        case when REVIEW_SCORES_CHECKIN !='NaN' then CAST(CAST(REVIEW_SCORES_CHECKIN AS NUMERIC) AS INT) else 0 end as REVIEW_SCORES_CHECKIN,
        case when REVIEW_SCORES_COMMUNICATION !='NaN' then CAST(CAST(REVIEW_SCORES_COMMUNICATION AS NUMERIC) AS INT) else 0 end as REVIEW_SCORES_COMMUNICATION,
        case when REVIEW_SCORES_VALUE !='NaN' then CAST(CAST(REVIEW_SCORES_VALUE AS NUMERIC) AS INT) else 0 end as REVIEW_SCORES_VALUE
    from source
)

select * from transformed





