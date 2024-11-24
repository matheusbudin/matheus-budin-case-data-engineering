-- 1_total number of Breweries per State

SELECT state, SUM(brewery_count) AS total_breweries
FROM gold.gold_brewery_aggregated
GROUP BY state
ORDER BY total_breweries DESC;
