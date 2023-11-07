SELECT
    listing_neighbourhood,
    TO_CHAR(scraped_date, 'MM/YYYY') AS "month/year",
	(COUNT(*) FILTER (WHERE has_availability = 't')::numeric /COUNT(*)::numeric)*100 AS "Active listings rate",
	MIN(price) FILTER (WHERE has_availability = 't') as "Minimum price for active listing",
	MAX(price) FILTER (WHERE has_availability = 't') as "Maximum price for active listing",
	PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) FILTER (WHERE has_availability = 't') AS "Median price for active listing",
	AVG(price) FILTER (WHERE has_availability = 't') as "Average price for active listing",
	COUNT(DISTINCT host_id) as "distinct hosts",
	(COUNT(DISTINCT host_id) FILTER (WHERE host_is_superhost = 't')::numeric /COUNT(DISTINCT host_id)::numeric)*100 AS "Superhost Rate",
	AVG(review_scores_rating) FILTER (WHERE has_availability = 't') as "Average of review_scores_rating for active listings",
	((COUNT(*) FILTER (WHERE has_availability = 't')::numeric - lag(COUNT(*) FILTER (WHERE has_availability = 't')) OVER (PARTITION BY listing_neighbourhood ORDER BY TO_CHAR(scraped_date, 'MM/YYYY'))::numeric) / nullif(lag(COUNT(*) FILTER (WHERE has_availability = 't')) OVER (PARTITION BY listing_neighbourhood ORDER BY  TO_CHAR(scraped_date, 'MM/YYYY'))::numeric,0))*100 AS "Percentage change for active listings",
	((COUNT(*) FILTER (WHERE has_availability = 'f')::numeric - lag(COUNT(*) FILTER (WHERE has_availability = 'f')) OVER (PARTITION BY listing_neighbourhood ORDER BY TO_CHAR(scraped_date, 'MM/YYYY'))::numeric) / nullif(lag(COUNT(*) FILTER (WHERE has_availability = 'f')) OVER (PARTITION BY listing_neighbourhood ORDER BY  TO_CHAR(scraped_date, 'MM/YYYY'))::numeric,0))*100 AS "Percentage change for inactive listings",
    SUM(30 - availability_30) FILTER (WHERE has_availability = 't') as "Total Number of stays",
	AVG((30 - availability_30)*price) as "Average Estimated revenue per active listings"
FROM {{ ref('facts_listings') }}
GROUP BY listing_neighbourhood, "month/year"
ORDER BY listing_neighbourhood, "month/year"

