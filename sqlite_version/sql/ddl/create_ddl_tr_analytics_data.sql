-- Create transform table for analytics data
-- Note: This script is intended for one-time schema creation or schema updates
DROP TABLE IF EXISTS tr_analytics_data;
CREATE TABLE IF NOT EXISTS tr_analytics_data (
  date TEXT,
  page_url TEXT,
  pageviews INTEGER,
  sessions INTEGER,
  conversions INTEGER,
  conversion_rate REAL,
  data_date TEXT,    -- Batch date of the process
  run_date TIMESTAMP -- Timestamp when data was loaded
);

-- Create an index on date and page_url for faster joins
CREATE INDEX IF NOT EXISTS idx_tr_analytics_date_page ON tr_analytics_data(date, page_url);
CREATE INDEX IF NOT EXISTS idx_tr_analytics_data_date ON tr_analytics_data(data_date);