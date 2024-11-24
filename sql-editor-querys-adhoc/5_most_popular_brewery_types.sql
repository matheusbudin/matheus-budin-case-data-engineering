--5_most_popular_brewery_types
WITH RankedBreweries AS (
    SELECT
        state,
        brewery_type,
        brewery_count,
        ROW_NUMBER() OVER(PARTITION BY state ORDER BY brewery_count DESC) AS rank
    FROM gold.gold_brewery_aggregated
)
SELECT
    brewery_type AS most_popular_brewery_type,
    SUM(brewery_count)
FROM RankedBreweries
WHERE rank = 1
GROUP BY brewery_type
ORDER BY SUM(brewery_count) DESC;