{{
    config(
        unique_key='LGA_CODE'
    )
}}

with

source  as (

    select * from "postgres"."raw"."lga_code"

),
transformed as (
    select
        lga_code,
        UPPER(lga_name) as lga_name 
    from source
)
select * from transformed
