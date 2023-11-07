{% snapshot host_snapshot %}

{{
        config(
          target_schema='raw',
          strategy="timestamp",
          unique_key="host_id",
          updated_at="scraped_date"
        )
    }}

select 
      distinct host_id,
      scraped_date,
      host_name,
      host_since,
      host_is_superhost,
      host_neighbourhood 
from {{ source('raw', 'listing') }}

{% endsnapshot %}