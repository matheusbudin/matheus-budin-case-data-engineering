--2_Top Brewery Types NationWide
SELECT brewery_type, SUM(brewery_count) AS total_breweries
FROM gold.gold_brewery_aggregated
GROUP BY brewery_type
ORDER BY total_breweries DESC;