#!/usr/bin/env python3
"""
SEO Assessment ETL Pipeline

This script orchestrates the complete ETL process for the SEO assessment:
1. Extract data from CSV files
2. Transform and clean the data
3. Join the datasets
4. Create the final fact table
5. Export results

The pipeline is configurable, idempotent, and includes comprehensive logging.
"""

import os
import sys
import argparse
import logging
import yaml
import glob
import csv
import sqlite3
from datetime import datetime
import hashlib
import traceback
from Utilities.ingest_csv_to_sqlite import ingest_csv_to_sqlite as util_ingest_csv_to_sqlite
from Utilities.execute_sqlite_sql import execute_sql_file

# Setup logging
def setup_logging(log_level, log_file=None):
    """Configure logging with the specified level and optional file output."""
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    
    # Create logger
    logger = logging.getLogger()
    logger.setLevel(numeric_level)
    
    # Clear existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Add console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(log_format))
    logger.addHandler(console_handler)
    
    # Add file handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file, mode='w')  # 'w' mode to overwrite instead of append
        file_handler.setFormatter(logging.Formatter(log_format))
        logger.addHandler(file_handler)
    
    return logger

# Load configuration
def load_config(config_path='config.yml'):
    """Load configuration from YAML file."""
    try:
        with open(config_path, 'r') as config_file:
            return yaml.safe_load(config_file)
    except Exception as e:
        logging.error(f"Failed to load configuration from {config_path}: {e}")
        sys.exit(1)

# Note: We're now using the execute_sql_file function from Utilities.execute_sqlite_sql instead
# This provides a unified SQL execution mechanism across the project

# These functions have been replaced by utility modules imported from Utilities

# Run transformation scripts
def run_transformations(config, data_date=None):
    """Run all transformation SQL scripts."""
    db_path = config['database']['path']
    transform_dir = config['sql']['transform_dir']
    
    # Get all transformation scripts
    transform_scripts = [
        os.path.join(transform_dir, config['sql']['transform_gsc']),
        os.path.join(transform_dir, config['sql']['transform_analytics']),
        os.path.join(transform_dir, config['sql']['transform_rank'])
    ]
    
    # Execute each script
    success_count = 0
    for script in transform_scripts:
        result = execute_sql_file(script, db_path, data_date)
        if result["status"] == "success":
            success_count += 1
    
    return success_count == len(transform_scripts)

# Run join scripts
def run_joins(config, data_date=None):
    """Run all join SQL scripts."""
    db_path = config['database']['path']
    join_dir = config['sql']['join_dir']
    
    # Get all join scripts
    join_scripts = [
        os.path.join(join_dir, config['sql']['join_gsc_analytics']),
        os.path.join(join_dir, config['sql']['join_gsc_rank'])
    ]
    
    # Execute each script
    success_count = 0
    for script in join_scripts:
        result = execute_sql_file(script, db_path, data_date)
        if result["status"] == "success":
            success_count += 1
    
    return success_count == len(join_scripts)

# Create fact table
def create_fact_table(config, data_date=None):
    """Create the final fact table."""
    db_path = config['database']['path']
    fact_dir = config['sql']['fact_dir']
    fact_script = os.path.join(fact_dir, config['sql']['fact_seo'])
    
    result = execute_sql_file(fact_script, db_path, data_date)
    return result["status"] == "success"

# Export fact table to CSV
def export_fact_table(config):
    """Export the fact table to CSV."""
    if not config['output'].get('export_csv', False):
        logging.info("CSV export disabled in configuration")
        return True
    
    try:
        db_path = config['database']['path']
        export_dir = config['output']['export_dir']
        
        # Create export directory if it doesn't exist
        os.makedirs(export_dir, exist_ok=True)
        
        # Connect to database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get column names
        cursor.execute("PRAGMA table_info(fact_seo_performance)")
        columns = [row[1] for row in cursor.fetchall()]
        
        # Export data
        export_path = os.path.join(export_dir, f"fact_seo_performance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        cursor.execute("SELECT * FROM fact_seo_performance")
        
        with open(export_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(columns)  # Write header
            writer.writerows(cursor.fetchall())  # Write data
        
        conn.close()
        
        logging.info(f"Exported fact table to {export_path}")
        return True
        
    except Exception as e:
        logging.error(f"Error exporting fact table: {e}")
        logging.debug(traceback.format_exc())
        return False

# Note: Schema creation has been moved to a separate script (seo_create_ddl.py)

# Main pipeline function
def run_pipeline(config, data_date=None):
    """Run the complete ETL pipeline."""
    try:
        # Start time
        start_time = datetime.now()
        logging.info(f"Starting SEO pipeline at {start_time}")
        
        # Use data_date from config if not provided as parameter
        if not data_date and 'data_date' in config.get('processing', {}):
            data_date = config['processing']['data_date']
        
        # Default to current date if still not set
        if not data_date:
            data_date = datetime.now().strftime('%Y-%m-%d')
            
        logging.info(f"Using data_date: {data_date}")
        
        # Note: Schema creation should be done once by running seo_create_ddl.py
        # This pipeline assumes the tables already exist
        
        # 1. Process datasets
        logging.info("Step 1: Processing datasets")
        
        # Process GSC data
        logging.info("Step 1.1: Processing GSC data")
        gsc_dir = os.path.join(config['input']['data_dir'], config['input']['gsc_dir'])
        gsc_prefix = config['input']['gsc_prefix']
        gsc_result = util_ingest_csv_to_sqlite(gsc_dir, gsc_prefix, 'stg_gsc_data', config['database']['path'], data_date)
        
        # Consider only "success" and "skipped" as successful outcomes
        # For "partial", we require at least one file to be successfully processed
        gsc_success = gsc_result["status"] in ["success", "skipped"] or \
                      (gsc_result["status"] == "partial" and gsc_result.get("succeeded", 0) > 0)
        
        # Fail fast if GSC data ingestion failed completely
        if not gsc_success:
            logging.error(f"GSC data ingestion failed: {gsc_result.get('message', 'Unknown error')}")
            logging.error("Pipeline execution stopped. Please fix data ingestion issues before proceeding.")
            return False
        
        # Process Analytics data
        logging.info("Step 1.2: Processing Analytics data")
        analytics_dir = os.path.join(config['input']['data_dir'], config['input']['analytics_dir'])
        analytics_prefix = config['input']['analytics_prefix']
        analytics_result = util_ingest_csv_to_sqlite(analytics_dir, analytics_prefix, 'stg_analytics_data', config['database']['path'], data_date)
        
        analytics_success = analytics_result["status"] in ["success", "skipped"] or \
                            (analytics_result["status"] == "partial" and analytics_result.get("succeeded", 0) > 0)
        
        # Fail fast if Analytics data ingestion failed completely
        if not analytics_success:
            logging.error(f"Analytics data ingestion failed: {analytics_result.get('message', 'Unknown error')}")
            logging.error("Pipeline execution stopped. Please fix data ingestion issues before proceeding.")
            return False
        
        # Process Rank data
        logging.info("Step 1.3: Processing Rank data")
        rank_dir = os.path.join(config['input']['data_dir'], config['input']['rank_dir'])
        rank_prefix = config['input']['rank_prefix']
        rank_result = util_ingest_csv_to_sqlite(rank_dir, rank_prefix, 'stg_rank_data', config['database']['path'], data_date)
        
        rank_success = rank_result["status"] in ["success", "skipped"] or \
                      (rank_result["status"] == "partial" and rank_result.get("succeeded", 0) > 0)
        
        # Fail fast if Rank data ingestion failed completely
        if not rank_success:
            logging.error(f"Rank data ingestion failed: {rank_result.get('message', 'Unknown error')}")
            logging.error("Pipeline execution stopped. Please fix data ingestion issues before proceeding.")
            return False
        
        # Log a warning if any dataset had partial success
        if gsc_result["status"] == "partial" or analytics_result["status"] == "partial" or rank_result["status"] == "partial":
            logging.warning("Some files failed to process completely. Check logs for details.")
        
        # 2. Run transformations
        logging.info("Step 2: Running transformations")
        transform_success = run_transformations(config, data_date)
        
        if not transform_success:
            logging.error("Transformation step failed")
            return False
        
        # 3. Run joins
        logging.info("Step 3: Running joins")
        join_success = run_joins(config, data_date)
        
        if not join_success:
            logging.error("Join step failed")
            return False
        
        # 4. Create fact table
        logging.info("Step 4: Creating fact table")
        fact_success = create_fact_table(config, data_date)
        
        if not fact_success:
            logging.error("Fact table creation failed")
            return False
        
        # 5. Export results
        logging.info("Step 5: Exporting results")
        export_success = export_fact_table(config)
        
        # End time and summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logging.info(f"Pipeline completed at {end_time} (duration: {duration:.2f} seconds)")
        
        return fact_success and export_success
        
    except Exception as e:
        logging.error(f"Pipeline failed with error: {e}")
        logging.debug(traceback.format_exc())
        return False

# Command-line interface
def main():
    parser = argparse.ArgumentParser(description='SEO Assessment ETL Pipeline')
    parser.add_argument('--config', default='config.yml', help='Path to configuration file')
    parser.add_argument('--log-level', default='INFO', help='Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)')
    parser.add_argument('--log-file', help='Path to log file (optional)')
    parser.add_argument('--data-date', help='Batch date for processing (YYYY-MM-DD format)')
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging(args.log_level, args.log_file)
    
    # Load configuration
    config = load_config(args.config)
    
    # Update log settings from config if not overridden
    if not args.log_file and 'log_file' in config.get('processing', {}):
        log_file = config['processing']['log_file']
        setup_logging(args.log_level, log_file)
    
    # Run pipeline
    success = run_pipeline(config, args.data_date)
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()