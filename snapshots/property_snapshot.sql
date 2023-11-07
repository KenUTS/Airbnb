{% snapshot property_snapshot %}

{{
        config(
          target_schema='raw',
          strategy="timestamp",
          unique_key="key_id",
          updated_at="scraped_date"
        )
    }}

select 
    distinct property_type, 
    dense_rank() over(order by property_type) as key_id,
    scraped_date
from {{ source('raw', 'listing') }}

{% endsnapshot %}