-- 4_Percentage Distribution of Brewery Types per State:
SELECT 
    state,
    brewery_type,
    brewery_count,
    ROUND((brewery_count * 100.0) / SUM(brewery_count) OVER(PARTITION BY state), 2) AS percentage
FROM budinworkspac.gold.gold_brewery_aggregated
ORDER BY state, percentage DESC;