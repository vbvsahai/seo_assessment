-- Note: This script is intended for one-time schema creation or schema updates
DROP TABLE IF EXISTS stg_gsc_data;
CREATE TABLE IF NOT EXISTS stg_gsc_data (
  date TEXT,
  query TEXT,
  page TEXT,
  clicks INTEGER,
  impressions INTEGER,
  ctr REAL,
  avg_position REAL,
  data_date TEXT,    -- Batch date of the process
  run_date TIMESTAMP -- Timestamp when data was loaded
);