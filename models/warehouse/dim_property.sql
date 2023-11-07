{{
    config(
        unique_key='property_id'
    )
}}

select * from {{ ref('property_stg') }}