# SEO Assessment Project - SQLite Version

This project implements an ETL pipeline for processing SEO data from multiple sources and creating a unified fact table for analysis. The pipeline extracts data from CSV files, transforms and cleans the data, joins related datasets, and loads the results into a SQLite database.

## Project Structure

```
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
```

## Data Model

The pipeline processes three main datasets:

1. **Google Search Console (GSC) Data**
   - Date, keyword (query), page URL, clicks, impressions, CTR, avg_position
   - Stored in `stg_gsc_data` table
   - Transformed in `tr_gsc_data` with `estimated_traffic` calculation

2. **Analytics Data**
   - Date, page URL, pageviews, sessions, conversions
   - Stored in `stg_analytics_data` table
   - Transformed in `tr_analytics_data` with `conversion_rate` calculation

3. **Rank Data**
   - Date, keyword, URL, rank, monthly_search_volume, CPC
   - Stored in `stg_rank_data` table
   - Transformed in `tr_rank_data` with rank categorization

These datasets are joined together to create:

1. **GSC-Analytics Join** (`int_gsc_analytics`)
   - Shows what keywords each page appears for in search results

2. **GSC-Rank Join** (`int_gsc_rank`)
   - Shows the differential between impressions in GSC and available search volume

3. **Final Fact Table** (`fact_seo_performance`)
   - Comprehensive view combining all required fields

## Pipeline Steps

The ETL pipeline performs the following steps:

1. **Extract**: CSV files are loaded into staging tables
   - Tracks processed files in `log_file_dtl` for idempotency
   - Handles missing files gracefully

2. **Transform**: Staging data is cleaned and enhanced
   - Calculates `estimated_traffic` for GSC data
   - Calculates `conversion_rate` for Analytics data
   - Cleans and standardizes formats

3. **Join**: Related datasets are combined
   - GSC data is joined with Analytics data
   - GSC data is joined with Rank data

4. **Load**: Final fact table is created
   - Contains all required fields from the specification
   - Can be exported to CSV for further analysis

## Usage

### Prerequisites

- Python 3.6 or higher
- Required Python packages: `pyyaml`

### Setup

1. Clone the repository
2. Install required packages:
   ```bash
   pip install pyyaml
   ```
3. Place your CSV files in the appropriate data directories:
   - `data/gsc_data/gsc_data*.csv`
   - `data/analytics_data/analytics_data*.csv`
   - `data/rank_data/rank_data*.csv`

### Running the Pipeline

One time run for schema creation:
```bash
   python seo_create_ddl.py
   ```

Run the complete pipeline with:

```bash
python seo_pipeline.py
```

You can customize the execution with these options:
- `--config`: Path to configuration file (default: `config.yml`)
- `--log-level`: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- `--log-file`: Path to log file

### create schema
python3 /Users/vasahai/Downloads/seo_assessment/sqlite_version/seo_create_ddl.py --config config.yml --log-level INFO
  --log-file create_ddl.log --data-date YYYY-MM-DD

### run pipeline for specific date
python3 /Users/vasahai/Downloads/seo_assessment/sqlite_version/seo_pipeline.py --config config.yml --log-level INFO --log-file 
  pipeline.log --data-date YYYY-MM-DD

### Individual Steps

You can also run individual steps:


1. Ingest a specific dataset:
   ```bash
   python Utilities/ingest_csv_to_sqlite.py data/gsc_data/ gsc_data stg_gsc_data
   python Utilities/ingest_csv_to_sqlite.py data/analytics_data/ analytics_data stg_analytics_data
   python Utilities/ingest_csv_to_sqlite.py data/rank_data/ rank_data stg_rank_data
   ```
2. Execute a specific SQL script:
   ```bash
   python Utilities/execute_sqlite_sql.py sql/transform/transform_gsc_data.sql
   ```

## Configuration

The pipeline is configured using the `config.yml` file. Key configuration options include:

- Database settings
- Input data directories and file prefixes
- SQL script locations
- Logging settings
- Export options

## Features

- **Idempotent**: Rerunning the pipeline does not duplicate data
- **Configurable**: All settings are in a single configuration file
- **Robust Error Handling**: Gracefully handles missing files and errors
- **Comprehensive Logging**: Detailed logs of all operations
- **Data Quality**: Cleans and validates input data
- **Flexible**: Can be run as a complete pipeline or step by step

## Output

The pipeline creates a SQLite database (`seo_assessment.db`) with the following key tables:

- Staging tables (`stg_*`): Raw data from CSV files
- Transformed tables (`tr_*`): Cleaned and enhanced data
- Intermediate tables (`int_*`): Joined datasets
- Fact table (`fact_seo_performance`): Final unified view

The fact table can also be exported as a CSV file to the `exports/` directory.

## Extending the Project

You can extend this project by:

1. Adding more data sources
2. Creating additional transformations
3. Building visualization capabilities
4. Implementing scheduling for regular updates
5. Adding more analytics queries

## Troubleshooting

If you encounter issues:

1. Check the log file for detailed error messages
2. Verify that input CSV files are in the correct format
3. Ensure the SQLite database file is writable
4. Check that all required directories exist

## License

This project is provided as-is for educational and demonstration purposes.
