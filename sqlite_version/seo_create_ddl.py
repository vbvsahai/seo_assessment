#!/usr/bin/env python3
"""
SEO Assessment Schema Creation Script

This script initializes the database schema by executing all DDL scripts in the sql/ddl directory.
It should be run ONLY in the following scenarios:
1. Initially to set up the database tables before running the ETL pipeline for the first time
2. When schema changes are needed (e.g., adding new columns, modifying data types)

IMPORTANT: Running this script will DROP and recreate tables, which will DELETE all existing data.
Do NOT run this script as part of the regular ETL process - it is separate by design.
"""

import os
import sys
import glob
import argparse
import logging
import yaml
from datetime import datetime
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
        file_handler = logging.FileHandler(log_file)
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

# Create database schema
def create_schema(config, data_date=None):
    """Create database schema by executing all DDL scripts."""
    logging.info("Creating database schema...")
    
    db_path = config['database']['path']
    ddl_dir = config['sql']['ddl_dir']
    
    # Get all DDL scripts
    ddl_scripts = glob.glob(os.path.join(ddl_dir, "*.sql"))
    
    if not ddl_scripts:
        logging.warning(f"No DDL scripts found in {ddl_dir}")
        return False
    
    # Sort scripts alphabetically for consistent execution order
    ddl_scripts.sort()
    
    logging.info(f"Found {len(ddl_scripts)} DDL scripts to execute")
    
    # Execute each script
    success_count = 0
    for script in ddl_scripts:
        logging.info(f"Executing DDL script: {os.path.basename(script)}")
        result = execute_sql_file(script, db_path, data_date)
        if result["status"] == "success":
            success_count += 1
    
    if success_count == len(ddl_scripts):
        logging.info("All schema creation scripts executed successfully")
        return True
    else:
        logging.error(f"Schema creation partially completed: {success_count} of {len(ddl_scripts)} scripts succeeded")
        return False

# Main function
def main():
    parser = argparse.ArgumentParser(description='SEO Assessment Schema Creation')
    parser.add_argument('--config', default='config.yml', help='Path to configuration file')
    parser.add_argument('--log-level', default='INFO', help='Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)')
    parser.add_argument('--log-file', help='Path to log file (optional)')
    parser.add_argument('--data-date', help='Batch date for processing (YYYY-MM-DD format)')
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level, args.log_file)
    
    # Print header
    logging.info("=" * 80)
    logging.info("SEO ASSESSMENT SCHEMA CREATION")
    logging.info("=" * 80)
    
    # Load configuration
    config = load_config(args.config)
    
    # Use data_date from config if not provided as parameter
    data_date = args.data_date
    if not data_date and 'data_date' in config.get('processing', {}):
        data_date = config['processing']['data_date']
    
    # Default to current date if still not set
    if not data_date:
        data_date = datetime.now().strftime('%Y-%m-%d')
        
    logging.info(f"Using data_date: {data_date}")
    
    # Create schema
    success = create_schema(config, data_date)
    
    # Exit with appropriate code
    if success:
        logging.info("Schema creation completed successfully")
        sys.exit(0)
    else:
        logging.error("Schema creation failed")
        sys.exit(1)

if __name__ == "__main__":
    main()