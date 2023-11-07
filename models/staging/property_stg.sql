{{
    config(
        unique_key='key_id'
    )
}}

with

source  as (

    select * from {{ ref('property_snapshot') }}

),

renamed as (
    select
        key_id as property_id,
        property_type as property_description,
        dbt_valid_from::date ,
        dbt_valid_to::date 
    from source
),

unknown as (
    select
        0 as property_id,
        'unknown' as property_description,
        '1900-01-01'::date  as dbt_valid_from,
        null::date as dbt_valid_to

)
select * from unknown
union all
select * from renamed
