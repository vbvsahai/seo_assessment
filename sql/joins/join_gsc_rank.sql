-- Join GSC data with Rank data
-- This shows the differential between impressions in GSC and available search volume
-- Uses DATA_DATE() function provided by execute_sqlite_sql.py

-- Delete existing records for the current data_date to handle reruns
DELETE FROM int_gsc_rank WHERE data_date = DATA_DATE();

-- Insert joined data for the current data_date
INSERT INTO int_gsc_rank (
    date,
    keyword,
    page_url,
    clicks,
    impressions,
    ctr,
    avg_position,
    estimated_traffic,
    rank,
    monthly_search_volume,
    cpc,
    rank_category,
    impression_share,
    data_date,
    run_date
)
SELECT
    g.date,
    g.keyword,
    g.page_url,
    g.clicks,
    g.impressions,
    g.ctr,
    g.avg_position,
    g.estimated_traffic,
    r.rank,
    r.monthly_search_volume,
    r.cpc,
    r.rank_category,
    -- Calculate the impression share (what percentage of available search volume is captured)
    CASE
        WHEN r.monthly_search_volume > 0 THEN
            ROUND(CAST(g.impressions AS REAL) / r.monthly_search_volume, 4)
        ELSE 0
    END AS impression_share,
    
    -- Audit columns - use GSC data_date if available, otherwise rank data_date
    COALESCE(g.data_date, r.data_date, DATA_DATE()) AS data_date,
    CURRENT_TIMESTAMP AS run_date
FROM
    (select * from tr_gsc_data where data_date = DATA_DATE()) g
LEFT JOIN
    (select * from tr_rank_data where data_date = DATA_DATE()) r
ON
    g.date = r.date
    AND g.keyword = r.keyword
    AND g.page_url = r.page_url