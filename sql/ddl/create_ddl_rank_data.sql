DROP TABLE IF EXISTS stg_rank_data;
CREATE TABLE IF NOT EXISTS stg_rank_data (
  date TEXT,
  keyword TEXT,
  url TEXT,
  rank INTEGER,
  monthly_search_volume INTEGER,
  cpc REAL,
  data_date TEXT,    -- Batch date of the process
  run_date TIMESTAMP -- Timestamp when data was loaded
);