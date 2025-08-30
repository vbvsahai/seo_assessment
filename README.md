sqlite_version/
├── sql/
│   ├── ddl/               # Schema definition scripts
│   │   ├── create_ddl_analytics_data.sql
│   │   ├── create_ddl_gsc_data.sql
│   │   ├── create_ddl_log_file_dtl.sql
│   │   └── create_ddl_rank_data.sql
│   ├── transform/         # Data transformation scripts
│   │   ├── transform_analytics_data.sql
│   │   ├── transform_gsc_data.sql
│   │   └── transform_rank_data.sql
│   ├── joins/             # Data joining scripts
│   │   ├── join_gsc_analytics.sql
│   │   └── join_gsc_rank.sql
│   └── fact/              # Fact table creation scripts
│       └── create_fact_seo_performance.sql
├── data/                  # Input data directories
│   ├── analytics_data/
│   ├── gsc_data/
│   └── rank_data/
├── Utilities/             # Utility scripts
│   ├── execute_sqlite_sql.py
│   └── ingest_csv_to_sqlite.py
├── exports/               # Exported data (created by the pipeline)
├── config.yml             # Configuration file
├── seo_pipeline.py        # Main pipeline script
└── README.md              # This documentation file
