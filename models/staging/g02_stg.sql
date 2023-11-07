{{
    config(
        unique_key='LGA_CODE_2016'
    )
}}

with

source  as (

    select * from "postgres"."raw"."census_g02"

),

transformed as (
    select
        cast(regexp_replace(lga_code_2016,'[^0-9]','','g') as int) as lga_code,
        Median_age_persons,	
        Median_mortgage_repay_monthly,	
        Median_tot_prsnl_inc_weekly,	
        Median_rent_weekly,	
        Median_tot_fam_inc_weekly,	
        Average_num_psns_per_bedroom,	
        Median_tot_hhd_inc_weekly,	
        Average_household_size
    from source
),
unknown as (
    select
        0 as lga_code,
        0 as Median_age_persons,	
        0 as Median_mortgage_repay_monthly,	
        0 as Median_tot_prsnl_inc_weekly,	
        0 as Median_rent_weekly,	
        0 as Median_tot_fam_inc_weekly,	
        0 as Average_num_psns_per_bedroom,	
        0 as Median_tot_hhd_inc_weekly,	
        0 as Average_household_size
)

select * from unknown
union all
select * from transformed
