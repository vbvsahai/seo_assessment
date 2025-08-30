-- Create transform table for GSC data
-- Note: This script is intended for one-time schema creation or schema updates
DROP TABLE IF EXISTS tr_gsc_data;
CREATE TABLE IF NOT EXISTS tr_gsc_data (
  date TEXT,
  keyword TEXT,
  page_url TEXT,
  clicks INTEGER,
  impressions INTEGER,
  ctr REAL,
  avg_position REAL,
  estimated_traffic REAL,
  data_date TEXT,    -- Batch date of the process
  run_date TIMESTAMP -- Timestamp when data was loaded
);

-- Create indices for faster joins
CREATE INDEX IF NOT EXISTS idx_tr_gsc_date_page ON tr_gsc_data(date, page_url);
CREATE INDEX IF NOT EXISTS idx_tr_gsc_date_keyword ON tr_gsc_data(date, keyword);
CREATE INDEX IF NOT EXISTS idx_tr_gsc_data_date ON tr_gsc_data(data_date);