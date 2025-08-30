-- Create transform table for rank data
-- Note: This script is intended for one-time schema creation or schema updates
DROP TABLE IF EXISTS tr_rank_data;
CREATE TABLE IF NOT EXISTS tr_rank_data (
  date TEXT,
  keyword TEXT,
  page_url TEXT,
  rank INTEGER,
  monthly_search_volume INTEGER,
  cpc REAL,
  rank_category TEXT,
  data_date TEXT,    -- Batch date of the process
  run_date TIMESTAMP -- Timestamp when data was loaded
);

-- Create indices for faster joins
CREATE INDEX IF NOT EXISTS idx_tr_rank_keyword ON tr_rank_data(date, keyword, page_url);
CREATE INDEX IF NOT EXISTS idx_tr_rank_page ON tr_rank_data(page_url);
CREATE INDEX IF NOT EXISTS idx_tr_rank_data_date ON tr_rank_data(data_date);