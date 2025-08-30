-- Create join table for GSC and Rank data
-- Note: This script is intended for one-time schema creation or schema updates
DROP TABLE IF EXISTS int_gsc_rank;
CREATE TABLE IF NOT EXISTS int_gsc_rank (
  date TEXT,
  keyword TEXT,
  page_url TEXT,
  clicks INTEGER,
  impressions INTEGER,
  ctr REAL,
  avg_position REAL,
  estimated_traffic REAL,
  rank INTEGER,
  monthly_search_volume INTEGER,
  cpc REAL,
  rank_category TEXT,
  impression_share REAL,
  data_date TEXT,    -- Batch date of the process
  run_date TIMESTAMP -- Timestamp when data was loaded
);

-- Create indices for faster joins
CREATE INDEX IF NOT EXISTS idx_int_gsc_rank_date ON int_gsc_rank(date);
CREATE INDEX IF NOT EXISTS idx_int_gsc_rank_keyword ON int_gsc_rank(keyword);
CREATE INDEX IF NOT EXISTS idx_int_gsc_rank_page ON int_gsc_rank(page_url);
CREATE INDEX IF NOT EXISTS idx_int_gsc_rank_data_date ON int_gsc_rank(data_date);