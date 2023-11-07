{{
    config(
        unique_key='SUBURB_NAME'
    )
}}

select * from {{ ref('suburb_stg') }}