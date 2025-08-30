DROP TABLE IF EXISTS stg_analytics_data;
CREATE TABLE IF NOT EXISTS stg_analytics_data (
  date TEXT,
  page TEXT,
  pageviews INTEGER,
  sessions INTEGER,
  conversions INTEGER,
  data_date TEXT,    -- Batch date of the process
  run_date TIMESTAMP -- Timestamp when data was loaded
);