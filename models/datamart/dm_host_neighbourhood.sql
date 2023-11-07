SELECT
    host_neighbourhood_lga,
    TO_CHAR(scraped_date, 'MM/YYYY') AS "month/year",
	COUNT(DISTINCT host_id) as "distinct hosts",
	SUM((30 - availability_30)*price) as "Estimated revenue",
	SUM((30 - availability_30)*price)/COUNT(DISTINCT host_id)  as "Estimated Revenue per host"
FROM {{ ref('facts_listings') }}
GROUP BY host_neighbourhood_lga , "month/year"
ORDER BY host_neighbourhood_lga , "month/year"
