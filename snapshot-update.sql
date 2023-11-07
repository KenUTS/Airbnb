Update host_snapshot t1
set dbt_valid_to = next_date
from (
select host_id, scraped_date, lead(scraped_date) over (partition by host_id order by scraped_date) as next_date
from host_snapshot) t2
where t1.host_id=t2.host_id and t1.scraped_date=t2.scraped_date;

Update property_snapshot t1
set dbt_valid_to = next_date
from (
select key_id, scraped_date, lead(scraped_date) over (partition by key_id order by scraped_date) as next_date
from property_snapshot) t2
where t1.key_id=t2.key_id and t1.scraped_date=t2.scraped_date;

Update room_snapshot t1
set dbt_valid_to = next_date
from (
select key_id, scraped_date, lead(scraped_date) over (partition by key_id order by scraped_date) as next_date
from room_snapshot) t2
where t1.key_id=t2.key_id and t1.scraped_date=t2.scraped_date;

