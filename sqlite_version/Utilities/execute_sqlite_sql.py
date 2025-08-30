#!/usr/bin/env python3
"""
Script to execute SQLite SQL files.
Usage: python execute_sqlite_sql.py path/to/sql/file.sql [--data-date YYYY-MM-DD]
"""

import os
import sys
import sqlite3
import argparse
import logging
from datetime import datetime
import re

# Configure logging
def setup_logging(log_level="INFO", log_file=None):
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

def execute_sql_file(sql_file_path, db_path='seo_assessment.db', data_date=None):
    """
    Execute a SQL file against SQLite database.
    
    Args:
        sql_file_path (str): Path to the SQL file to execute
        db_path (str): Path to the SQLite database file
        data_date (str): Batch date to use for the data_date column (YYYY-MM-DD format)
    
    Returns:
        dict: Result of the query execution with status
    """
    try:
        # Check if file exists
        if not os.path.exists(sql_file_path):
            return {
                "status": "error",
                "message": f"SQL file not found: {sql_file_path}"
            }
        
        # Set default data_date if not provided
        if not data_date:
            data_date = datetime.now().strftime('%Y-%m-%d')
            
        # Read SQL content from file
        with open(sql_file_path, 'r') as sql_file:
            sql_content = sql_file.read()
            
        # Add SQL variable declarations for data_date and run_date
        # These can be referenced in SQL scripts as :data_date and CURRENT_TIMESTAMP
        variable_declarations = f"""
-- Set data_date parameter to '{data_date}'
"""
        # Combine the variable declarations with the original SQL content
        sql_content = variable_declarations + sql_content
        
        # Connect to SQLite database
        conn = sqlite3.connect(db_path)
        
        # Enable parameter substitution
        conn.create_function("DATA_DATE", 0, lambda: data_date)
        
        cursor = conn.cursor()
        
        logging.info(f"Executing SQL file: {sql_file_path}")
        logging.info(f"Using database: {db_path}")
        logging.info(f"Using data_date: {data_date}")
        
        # Execute the query with parameter binding
        cursor.executescript(sql_content)
        
        # Commit changes
        conn.commit()
        
        # Close connection
        conn.close()
        
        logging.info(f"SQL execution completed successfully: {os.path.basename(sql_file_path)}")
        return {
            "status": "success",
            "file_name": os.path.basename(sql_file_path),
            "data_date": data_date
        }
        
    except Exception as e:
        error_message = str(e)
        logging.error(f"Error executing SQL {sql_file_path}: {error_message}")
        
        return {
            "status": "error",
            "message": error_message,
            "file_name": os.path.basename(sql_file_path) if sql_file_path else "unknown"
        }

def main():
    parser = argparse.ArgumentParser(description='Execute SQLite SQL files')
    parser.add_argument('sql_file', help='Path to the SQL file to execute')
    parser.add_argument('--db', default='seo_assessment.db', help='Path to the SQLite database file')
    parser.add_argument('--data-date', help='Batch date for the data_date column (YYYY-MM-DD format)')
    parser.add_argument('--log-level', default='INFO', help='Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)')
    parser.add_argument('--log-file', help='Path to log file (optional)')
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level, args.log_file)
    
    result = execute_sql_file(args.sql_file, args.db, args.data_date)
    
    if result["status"] == "success":
        logging.info("SQL execution completed successfully")
        sys.exit(0)
    else:
        logging.error(f"SQL execution failed: {result.get('message', 'Unknown error')}")
        sys.exit(1)

if __name__ == "__main__":
    main()