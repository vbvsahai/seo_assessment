-- Join GSC data with Analytics data
-- This shows what keywords each page appears for in search results
-- Uses DATA_DATE() function provided by execute_sqlite_sql.py

-- Delete existing records for the current data_date to handle reruns
DELETE FROM int_gsc_analytics WHERE data_date = DATA_DATE();

-- Insert joined data for the current data_date
INSERT INTO int_gsc_analytics (
    date,
    keyword,
    page_url,
    clicks,
    impressions,
    ctr,
    avg_position,
    estimated_traffic,
    pageviews,
    sessions,
    conversions,
    conversion_rate,
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
    a.pageviews,
    a.sessions,
    a.conversions,
    a.conversion_rate,
    COALESCE(g.data_date, a.data_date, DATA_DATE()) AS data_date,
    CURRENT_TIMESTAMP AS run_date
FROM
    (select * from tr_gsc_data where data_date = DATA_DATE())  g
LEFT JOIN
    (select * from tr_analytics_data where data_date = DATA_DATE()) a
ON
    g.date = a.date
    AND g.page_url = a.page_url