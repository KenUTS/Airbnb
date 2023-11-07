{{
    config(
        unique_key='room_id'
    )
}}

select * from {{ ref('room_stg') }}