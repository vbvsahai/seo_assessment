-- SQL Script to create the log_file_dtl table
-- Note: This script is intended for one-time schema creation or schema updates
DROP TABLE IF EXISTS log_file_dtl;
CREATE TABLE IF NOT EXISTS log_file_dtl (
  file_id TEXT,
  file_name TEXT,
  status TEXT,
  created_ts TIMESTAMP,
  created_user TEXT,
  data_date TEXT,    -- Batch date of the process
  run_date TIMESTAMP -- Timestamp when record was created
);