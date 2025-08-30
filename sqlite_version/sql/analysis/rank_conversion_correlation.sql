-- Analysis 3: Correlation between rank and conversion_rate
-- This query examines how search ranking correlates with conversion rates
-- It groups keywords/pages into rank buckets to identify optimal ranking targets

-- Calculate average conversion rate by rank bucket
SELECT 
    rank_bucket,
    COUNT(*) AS keyword_page_combinations,
    AVG(rank) AS avg_rank,
    AVG(conversion_rate) AS avg_conversion_rate,
    SUM(sessions) AS total_sessions,
    SUM(conversions) AS total_conversions,
    CASE 
        WHEN SUM(sessions) > 0 THEN ROUND(CAST(SUM(conversions) AS REAL) / SUM(sessions), 4)
        ELSE 0 
    END AS aggregate_conversion_rate
FROM 
    (
    SELECT 
        CASE 
            WHEN rank BETWEEN 1 AND 3 THEN '1-3 (Top 3)'
            WHEN rank BETWEEN 4 AND 10 THEN '4-10 (First Page)'
            WHEN rank BETWEEN 11 AND 20 THEN '11-20 (Second Page)'
            WHEN rank BETWEEN 21 AND 50 THEN '21-50 (Top 50)'
            WHEN rank > 50 THEN '51+ (Beyond Top 50)'
            ELSE 'Not Ranked'
        END AS rank_bucket,
        page_url,
        keyword,
        rank,
        conversion_rate,
        sessions,
        conversions
    FROM 
        fact_seo_performance
    WHERE 
        sessions > 10  -- Minimum sessions for statistical significance
        AND rank IS NOT NULL
    ) rank_buckets
GROUP BY 
    rank_bucket
ORDER BY 
    CASE 
        WHEN rank_bucket = '1-3 (Top 3)' THEN 1
        WHEN rank_bucket = '4-10 (First Page)' THEN 2
        WHEN rank_bucket = '11-20 (Second Page)' THEN 3
        WHEN rank_bucket = '21-50 (Top 50)' THEN 4
        WHEN rank_bucket = '51+ (Beyond Top 50)' THEN 5
        ELSE 6
    END;