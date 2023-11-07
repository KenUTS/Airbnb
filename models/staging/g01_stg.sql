{{
    config(
        unique_key='LGA_CODE_2016'
    )
}}

with

source  as (

    select * from "postgres"."raw"."census_g01"

),

transformed as (
    select
        cast(regexp_replace(lga_code_2016,'[^0-9]','','g') as int) as lga_code,
        Tot_P_P,
        Age_0_4_yr_P,
        Age_5_14_yr_P,
        Age_15_19_yr_P,
        Age_20_24_yr_P,
        Age_25_34_yr_P,
        Age_35_44_yr_P,
        Age_45_54_yr_P,
        Age_55_64_yr_P,
        Age_65_74_yr_P,
        Age_75_84_yr_P,
        Age_85ov_P
    from source
),

unknown as (
    select
        0 as lga_code,
        0 as Tot_P_P,
        0 as Age_0_4_yr_P,
        0 as Age_5_14_yr_P,
        0 as Age_15_19_yr_P,
        0 as Age_20_24_yr_P,
        0 as Age_25_34_yr_P,
        0 as Age_35_44_yr_P,
        0 as Age_45_54_yr_P,
        0 as Age_55_64_yr_P,
        0 as Age_65_74_yr_P,
        0 as Age_75_84_yr_P,
        0 as Age_85ov_P
)
select * from unknown
union all
select * from transformed
