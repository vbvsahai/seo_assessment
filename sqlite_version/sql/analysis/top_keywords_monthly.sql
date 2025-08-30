-- Analysis 1: Top 10 keywords by clicks and conversions each month
-- This query identifies the top 10 keywords by clicks and conversions for each month
-- It shows both rankings separately to compare traffic drivers vs. conversion drivers

-- Top keywords by clicks
SELECT month,keyword,total_clicks,total_conversions, 'Clicks' AS ranking_type, clicks_rank as rank
FROM (
SELECT 
month,keyword,
total_clicks,
total_conversions,
ROW_NUMBER() OVER (PARTITION BY month ORDER BY total_clicks DESC) AS clicks_rank
FROM 
(
SELECT strftime('%Y-%m', date) AS month,keyword,SUM(clicks) AS total_clicks,
SUM(conversions) AS total_conversions
FROM fact_seo_performance
WHERE keyword IS NOT NULL 
GROUP BY month, keyword ) monthly_data
) clicks_ranking
WHERE 
    clicks_rank <= 10

UNION ALL

SELECT month,keyword,total_clicks,total_conversions, 'Conversions' AS ranking_type, conversions_rank as rank
FROM (
SELECT 
month,keyword,
total_clicks,
total_conversions,
ROW_NUMBER() OVER (PARTITION BY month ORDER BY total_conversions DESC) AS conversions_rank
FROM 
(
SELECT strftime('%Y-%m', date) AS month,keyword,SUM(clicks) AS total_clicks,
SUM(conversions) AS total_conversions
FROM fact_seo_performance
WHERE keyword IS NOT NULL 
GROUP BY month, keyword ) monthly_data
) conversions_ranking
WHERE 
conversions_rank <= 10

order by month, ranking_type, rank;