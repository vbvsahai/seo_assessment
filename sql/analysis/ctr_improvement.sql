-- Analysis 2: Pages with highest CTR improvement (last 7 days vs previous 7)
-- This query compares CTR performance over the last 7 days with the previous 7 days
-- It identifies pages showing the greatest CTR improvement, which may indicate content or SERP improvements

SELECT 
    r.page_url,
    r.recent_ctr,
    p.previous_ctr,
    ROUND(r.recent_ctr - p.previous_ctr, 4) AS ctr_change,
    CASE 
        WHEN p.previous_ctr > 0 
        THEN ROUND(((r.recent_ctr - p.previous_ctr) / p.previous_ctr) * 100, 2)
        ELSE NULL
    END AS percentage_change,
    r.recent_clicks,
    r.recent_impressions,
    p.previous_clicks,
    p.previous_impressions
FROM 
    -- Recent 7 days data
    (SELECT 
        page_url,
        SUM(clicks) AS recent_clicks,
        SUM(impressions) AS recent_impressions,
        CASE 
            WHEN SUM(impressions) > 0 THEN ROUND(CAST(SUM(clicks) AS REAL) / SUM(impressions), 4)
            ELSE 0 
        END AS recent_ctr
    FROM 
        fact_seo_performance
    WHERE 
        date BETWEEN date((SELECT MAX(date) FROM fact_seo_performance), '-6 days') 
                 AND (SELECT MAX(date) FROM fact_seo_performance)
        AND impressions > 0
    GROUP BY 
        page_url
    ) r
JOIN 
    -- Previous 7 days data
    (SELECT 
        page_url,
        SUM(clicks) AS previous_clicks,
        SUM(impressions) AS previous_impressions,
        CASE 
            WHEN SUM(impressions) > 0 THEN ROUND(CAST(SUM(clicks) AS REAL) / SUM(impressions), 4)
            ELSE 0 
        END AS previous_ctr
    FROM 
        fact_seo_performance
    WHERE 
        date BETWEEN date((SELECT MAX(date) FROM fact_seo_performance), '-13 days') 
                 AND date((SELECT MAX(date) FROM fact_seo_performance), '-7 days')
        AND impressions > 0
    GROUP BY 
        page_url
    ) p ON r.page_url = p.page_url
WHERE 
    r.recent_impressions >= 100  -- Minimum impressions for statistical significance
    AND p.previous_impressions >= 100
ORDER BY 
    ctr_change DESC
LIMIT 20;