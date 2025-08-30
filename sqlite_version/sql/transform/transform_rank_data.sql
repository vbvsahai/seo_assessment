-- Transform rank data
-- Clean data and normalize fields
-- Uses DATA_DATE() function provided by execute_sqlite_sql.py

-- Delete existing records for the current data_date to handle reruns
DELETE FROM tr_rank_data WHERE data_date = DATA_DATE();

-- Insert transformed data for the current data_date
INSERT INTO tr_rank_data (
    date,
    keyword,
    page_url,
    rank,
    monthly_search_volume,
    cpc,
    rank_category,
    data_date,
    run_date
)
select date,
    keyword,
    page_url,
    rank,
    monthly_search_volume,
    cpc,
       -- Add rank category for analysis
    CASE
        WHEN rank BETWEEN 1 AND 3 THEN 'Top 3'
        WHEN rank BETWEEN 4 AND 10 THEN 'First Page'
        WHEN rank BETWEEN 11 AND 20 THEN 'Second Page'
        WHEN rank BETWEEN 21 AND 50 THEN 'Top 50'
        ELSE 'Below Top 50'
    END AS rank_category,
    data_date,
     CURRENT_TIMESTAMP AS run_date from
(SELECT
    -- Convert and standardize date format
    DATE(date) AS date,
    
    -- Clean and normalize keyword and URL
    LOWER(TRIM(keyword)) AS keyword,
    LOWER(TRIM(url)) AS page_url,
    
    -- Ensure numeric values are valid
    MIN(COALESCE(rank,0)) AS rank,
    
    -- Handle NULL or negative search volume
    MAX(COALESCE(monthly_search_volume, 0)) AS monthly_search_volume,
    
    -- Handle NULL or negative CPC
    MAX(COALESCE(cpc, 0)) AS cpc,

    data_date
FROM
    stg_rank_data
WHERE
    -- Filter out invalid data
    date IS NOT NULL
    AND keyword IS NOT NULL
    AND url IS NOT NULL
    -- Filter by data_date if provided in execute_sqlite_sql.py
    AND data_date = DATA_DATE() 
    group by date, keyword, page_url, data_date);