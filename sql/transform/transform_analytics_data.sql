-- Delete existing records for the current data_date to handle reruns
DELETE FROM tr_analytics_data WHERE data_date = DATA_DATE();

-- Insert transformed data for the current data_date
INSERT INTO tr_analytics_data (
    date,
    page_url,
    pageviews,
    sessions,
    conversions,
    conversion_rate,
    data_date,
    run_date
)
select date, page_url,pageviews, sessions,  conversions, 
 CASE WHEN sessions > 0 
 THEN ROUND(CAST(conversions AS REAL) / CAST(sessions AS REAL), 4) 
 ELSE 0.0 END AS conversion_rate, -- Calculate conversion_rate (conversions / sessions)
data_date, CURRENT_TIMESTAMP AS run_date
from (
SELECT 
DATE(date) AS date, -- Convert and standardize date format
LOWER(TRIM(page)) AS page_url,  -- Clean and normalize page URL
SUM(MAX(0, COALESCE(pageviews, 0))) AS pageviews, -- Ensure numeric values are not negative
SUM(MAX(0, COALESCE(sessions, 0))) AS sessions, -- Ensure numeric values are not negative
SUM(COALESCE(conversions, 0)) AS conversions, -- Handle NULL conversions (set to 0)
data_date
FROM stg_analytics_data
WHERE date IS NOT NULL AND page IS NOT NULL AND data_date = DATA_DATE()
GROUP BY date, page_url, data_date
);