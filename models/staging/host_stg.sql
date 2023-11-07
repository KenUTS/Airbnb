{{
    config(
        unique_key='host_id'
    )
}}

with

source  as (

    select * from {{ ref('host_snapshot') }}

),

transformed as (
    select
        cast(host_id as Bigint) as host_id ,
        case when host_name !='NaN' then host_name else 'unknown' end as host_name,
        case when host_since !='NaN' then to_date(host_since,'DD/MM/YY') else null::date end as host_since,
        case when host_is_superhost !='NaN' then host_is_superhost else 'unknown' end as host_is_superhost,
        case when host_neighbourhood !='NaN' then UPPER(host_neighbourhood) else 'unknown' end as host_neighbourhood,
        dbt_valid_from::date,
        dbt_valid_to::date 
    from source
),

unknown as (
    select
        0 as host_id ,
        'unknown' as host_name,
        null::date as host_since,
        'unknown' as host_is_superhost,
        'unknown' as host_neighbourhood,
        '1900-01-01'::date as dbt_valid_from,
        null::date as dbt_valid_to 
        

)
select * from unknown
union all
select * from transformed
