{{
    config(
        unique_key='host_id'
    )
}}

select * from {{ ref('host_stg') }}