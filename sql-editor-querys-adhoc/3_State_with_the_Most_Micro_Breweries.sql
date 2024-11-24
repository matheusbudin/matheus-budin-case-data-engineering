SELECT state, SUM(brewery_count)
FROM gold.gold_brewery_aggregated
WHERE brewery_type = 'micro'
GROUP BY state
ORDER BY SUM(brewery_count) DESC;