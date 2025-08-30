-- Create join table for GSC and Analytics data
-- Note: This script is intended for one-time schema creation or schema updates
DROP TABLE IF EXISTS int_gsc_analytics;
CREATE TABLE IF NOT EXISTS int_gsc_analytics (
  date TEXT,
  keyword TEXT,
  page_url TEXT,
  clicks INTEGER,
  impressions INTEGER,
  ctr REAL,
  avg_position REAL,
  estimated_traffic REAL,
  pageviews INTEGER,
  sessions INTEGER,
  conversions INTEGER,
  conversion_rate REAL,
  data_date TEXT,    -- Batch date of the process
  run_date TIMESTAMP -- Timestamp when data was loaded
);

-- Create indices for faster joins
CREATE INDEX IF NOT EXISTS idx_int_gsc_analytics_date ON int_gsc_analytics(date);
CREATE INDEX IF NOT EXISTS idx_int_gsc_analytics_page ON int_gsc_analytics(page_url);
CREATE INDEX IF NOT EXISTS idx_int_gsc_analytics_keyword ON int_gsc_analytics(keyword);
CREATE INDEX IF NOT EXISTS idx_int_gsc_analytics_data_date ON int_gsc_analytics(data_date);