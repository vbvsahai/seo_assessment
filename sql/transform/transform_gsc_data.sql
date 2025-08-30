-- Transform GSC data
-- Clean data and add estimated_traffic field
-- estimated_traffic is calculated based on clicks, impressions, and position
-- Uses DATA_DATE() function provided by execute_sqlite_sql.py

-- Delete existing records for the current data_date to handle reruns
DELETE FROM tr_gsc_data WHERE data_date = DATA_DATE();

-- Insert transformed data for the current data_date
INSERT INTO tr_gsc_data (
    date,
    keyword,
    page_url,
    clicks,
    impressions,
    ctr,
    avg_position,
    estimated_traffic,
    data_date,
    run_date
)
select  date,
    keyword,
    page_url,
    clicks,
    impressions,
     CASE WHEN impressions > 0 
 THEN ROUND(CAST(clicks AS REAL) / CAST(impressions AS REAL), 4)  ELSE 0.0 END AS ctr,
    avg_position,
    -- Calculate estimated_traffic (impressions * CTR * position factor)
    -- Position factor decreases as position gets worse (higher number)
    -- Formula: impressions * CTR * (1 / (1 + ln(avg_position)))
    CASE
        WHEN impressions > 0 AND avg_position > 0 THEN
            ROUND(impressions * (CASE WHEN impressions > 0 THEN ROUND(CAST(clicks AS REAL) / CAST(impressions AS REAL), 4) ELSE 0.0 END) * (1.0 / (1.0 + 0.1 * avg_position)), 2)
        ELSE 0
    END AS estimated_traffic,
    data_date,
    CURRENT_TIMESTAMP AS run_date from
(SELECT
    -- Convert and standardize date format
    DATE(date) AS date,  
    -- Clean and normalize query and page fields
    LOWER(TRIM(query)) AS keyword,
    LOWER(TRIM(page)) AS page_url,  
    -- Keep numeric metrics, ensuring they are not negative
    sum(MAX(0, coalesce(clicks, 0))) AS clicks,
    sum(MAX(0, coalesce(impressions, 0))) AS impressions,
    MAX(COALESCE(avg_position, 0)) AS avg_position,  
    -- Audit columns
    data_date
FROM
    stg_gsc_data
WHERE
    -- Filter out invalid data
    date IS NOT NULL
    AND query IS NOT NULL
    AND page IS NOT NULL
    -- Filter by data_date if provided in execute_sqlite_sql.py
    AND data_date = DATA_DATE()
    group by date, keyword, page_url, data_date);