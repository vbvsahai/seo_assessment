-- Create fact table for SEO performance data
-- Note: This script is intended for one-time schema creation or schema updates
DROP TABLE IF EXISTS fact_seo_performance;
CREATE TABLE IF NOT EXISTS fact_seo_performance (
  date TEXT,
  keyword TEXT,
  page_url TEXT,
  clicks INTEGER,
  impressions INTEGER,
  avg_position REAL,
  rank INTEGER,
  pageviews INTEGER,
  sessions INTEGER,
  conversions INTEGER,
  monthly_search_volume INTEGER,
  cpc REAL,
  estimated_traffic REAL,
  conversion_rate REAL,
  data_date TEXT,    -- Batch date of the process
  run_date TIMESTAMP -- Timestamp when data was loaded
);

-- Create indices for faster queries
CREATE INDEX IF NOT EXISTS idx_fact_seo_date ON fact_seo_performance(date);
CREATE INDEX IF NOT EXISTS idx_fact_seo_keyword ON fact_seo_performance(keyword);
CREATE INDEX IF NOT EXISTS idx_fact_seo_page ON fact_seo_performance(page_url);
CREATE INDEX IF NOT EXISTS idx_fact_seo_data_date ON fact_seo_performance(data_date);