DELETE FROM fact_seo_performance WHERE data_date = DATA_DATE();

-- Insert fact table data for the current data_date
INSERT INTO fact_seo_performance (
    date,
    keyword,
    page_url,
    clicks,
    impressions,
    avg_position,
    rank,
    pageviews,
    sessions,
    conversions,
    monthly_search_volume,
    cpc,
    estimated_traffic,
    conversion_rate,
    data_date,
    run_date
)
SELECT

    COALESCE(ga.date, gr.date) AS date,
    COALESCE(ga.keyword, gr.keyword) AS keyword,
    COALESCE(ga.page_url, gr.page_url) AS page_url,
    COALESCE(ga.clicks, gr.clicks, 0) AS clicks,
    COALESCE(ga.impressions, gr.impressions, 0) AS impressions,
    COALESCE(ga.avg_position, gr.avg_position, 0) AS avg_position,
    COALESCE(gr.rank, 0) AS rank,
    COALESCE(ga.pageviews, 0) AS pageviews,
    COALESCE(ga.sessions, 0) AS sessions,
    COALESCE(ga.conversions, 0) AS conversions,
    COALESCE(gr.monthly_search_volume, 0) AS monthly_search_volume,
    COALESCE(gr.cpc, 0) AS cpc,
    COALESCE(ga.estimated_traffic, gr.estimated_traffic, 0) AS estimated_traffic,
    COALESCE(ga.conversion_rate, 0) AS conversion_rate,
    -- Audit columns - use data_date from input tables if available
    COALESCE(ga.data_date, gr.data_date, DATA_DATE()) AS data_date,
    CURRENT_TIMESTAMP AS run_date
FROM
    -- Start with the GSC-Analytics join
    (select * from int_gsc_analytics where data_date = DATA_DATE())   ga
FULL OUTER JOIN
    (select * from int_gsc_rank where data_date = DATA_DATE()) gr
ON
    ga.date = gr.date
    AND ga.keyword = gr.keyword
    AND ga.page_url = gr.page_url