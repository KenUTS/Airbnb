{{
    config(
        unique_key='SUBURB_NAME'
    )
}}

with

source  as (

    select * from "postgres"."raw"."lga_suburb"

)


select * from source
