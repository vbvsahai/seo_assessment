#!/usr/bin/env python3
"""
Script to ingest CSV files to SQLite tables.
This script:
1. Takes a directory path and file name prefix
2. Finds all matching CSV files in the directory
3. Checks which files are already ingested by querying the log_file_dtl table
4. Ingests only new files and logs successful ingestions to the log_file_dtl table
"""

import os
import sys
import glob
import hashlib
import argparse
import sqlite3
import csv
import logging
# uuid import is not needed
from datetime import datetime

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

def get_file_id(file_path):
    """
    Generate a file_id based on the file name using MD5 hash.
    
    Args:
        file_path (str): Path to the file
    
    Returns:
        str: MD5 hash of the file name
    """
    file_name = os.path.basename(file_path)
    return hashlib.md5(file_name.encode()).hexdigest()

def get_already_ingested_files(conn):
    """
    Get a list of files that have already been ingested successfully.
    
    Args:
        conn (sqlite3.Connection): SQLite connection
    
    Returns:
        list: List of file_name values from log_file_dtl with completed status
    """
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT file_name FROM log_file_dtl WHERE status = 'completed'")
        results = cursor.fetchall()
        
        # Extract file names from the query results
        ingested_files = [row[0] for row in results]
        return ingested_files
    except Exception as e:
        logging.warning(f"Could not query log_file_dtl table: {e}")
        logging.debug("This may be normal if the table doesn't exist yet.")
        return []

def log_ingestion(conn, file_path, status, data_date=None):
    """
    Log file ingestion in the log_file_dtl table.
    
    Args:
        conn (sqlite3.Connection): SQLite connection
        file_path (str): Path to the file
        status (str): Status of the ingestion (completed or failed)
        data_date (str): Batch date for the data_date column (YYYY-MM-DD format)
    """
    try:
        cursor = conn.cursor()
        file_id = get_file_id(file_path)
        file_name = file_path
        current_time = datetime.now().isoformat()
        user = "admin"
        
        # Use current date for data_date if not provided
        if not data_date:
            data_date = datetime.now().strftime('%Y-%m-%d')
        
        # Current timestamp for run_date
        run_date = datetime.now().isoformat()
        
        cursor.execute(
            "INSERT INTO log_file_dtl (file_id, file_name, status, created_ts, created_user, data_date, run_date) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (file_id, file_name, status, current_time, user, data_date, run_date)
        )
        conn.commit()
        logging.info(f"Logged {status} ingestion for {file_name} in log_file_dtl table (data_date: {data_date})")
    except Exception as e:
        logging.warning(f"Could not log to log_file_dtl table: {e}")
        logging.debug("This may be normal if the table doesn't exist yet.")

def get_column_names(file_path):
    """
    Get column names from the CSV file.
    
    Args:
        file_path (str): Path to the CSV file
    
    Returns:
        list: List of column names
    """
    with open(file_path, 'r', newline='') as csvfile:
        reader = csv.reader(csvfile)
        return next(reader)  # Get the header row

def ingest_csv_to_sqlite(directory_path, file_prefix, table_name, db_path='seo_assessment.db', data_date=None):
    """
    Find and ingest matching CSV files to a SQLite table if they haven't been ingested already.
    
    Args:
        directory_path (str): Path to the directory containing CSV files
        file_prefix (str): Prefix of the CSV files to ingest
        table_name (str): Name of the SQLite table
        db_path (str): Path to the SQLite database file
        data_date (str): Batch date for the data_date column (YYYY-MM-DD format)
    
    Returns:
        dict: Result of the ingestion
    """
    # Use current date for data_date if not provided
    if not data_date:
        data_date = datetime.now().strftime('%Y-%m-%d')
    try:
        # Check if directory exists
        if not os.path.exists(directory_path) or not os.path.isdir(directory_path):
            return {
                "status": "error",
                "message": f"Directory not found: {directory_path}"
            }
        
        # Find matching CSV files
        file_pattern = os.path.join(directory_path, f"{file_prefix}*.csv")
        matching_files = glob.glob(file_pattern)
        
        if not matching_files:
            return {
                "status": "warning",
                "message": f"No files matching pattern {file_pattern} found"
            }
        
        # Connect to SQLite database
        conn = sqlite3.connect(db_path)
        
        # Get list of already ingested files
        ingested_files = get_already_ingested_files(conn)
        
        # Filter out already ingested files
        new_files = []
        for file_path in matching_files:
            if file_path not in ingested_files:
                new_files.append(file_path)
        
        if not new_files:
            conn.close()
            logging.info(f"All {len(matching_files)} matching files have already been ingested.")
            return {
                "status": "skipped",
                "message": "All matching files already ingested",
                "matched_files": len(matching_files)
            }
        
        logging.info(f"Found {len(new_files)} new files to ingest out of {len(matching_files)} matching files")
        
        success_count = 0
        error_count = 0
        results = []
        
        # Process each new file
        for file_path in new_files:
            try:
                logging.info(f"Ingesting {file_path} to {table_name}")
                
                # Get column names from the CSV file
                columns = get_column_names(file_path)
                
                # Create cursor
                cursor = conn.cursor()
                
                # Read CSV file
                with open(file_path, 'r', newline='') as csvfile:
                    reader = csv.reader(csvfile)
                    next(reader)  # Skip the header row
                    
                    # Prepare placeholders for the SQL query (including audit columns)
                    csv_placeholders = ', '.join(['?' for _ in columns])
                    
                    # Get table schema to determine the total number of columns needed
                    cursor.execute(f"PRAGMA table_info({table_name})")
                    table_columns = cursor.fetchall()
                    
                    # Determine if we need to add audit columns
                    has_audit_columns = any(col[1] == 'data_date' for col in table_columns)
                    
                    if has_audit_columns:
                        # We have audit columns - need to add values for data_date and run_date
                        run_date = datetime.now().isoformat()
                        
                        # Insert data row by row with audit columns
                        for row in reader:
                            # Add data_date and run_date to each row
                            full_row = list(row) + [data_date, run_date]
                            all_placeholders = csv_placeholders + ', ?, ?'
                            cursor.execute(f"INSERT INTO {table_name} VALUES ({all_placeholders})", full_row)
                    else:
                        # No audit columns - just insert the data as is
                        for row in reader:
                            cursor.execute(f"INSERT INTO {table_name} VALUES ({csv_placeholders})", row)
                
                # Commit changes
                conn.commit()
                
                # Log successful ingestion
                log_ingestion(conn, file_path, "completed", data_date)
                
                success_count += 1
                results.append({
                    "file": file_path,
                    "status": "success"
                })
                
                logging.info(f"Loaded data into {table_name}")
                
            except Exception as e:
                error_message = str(e)
                logging.error(f"Error ingesting {file_path}: {error_message}")
                
                # Log failed ingestion
                try:
                    log_ingestion(conn, file_path, "failed", data_date)
                except Exception:
                    pass  # Ignore errors in logging failure
                
                error_count += 1
                results.append({
                    "file": file_path,
                    "status": "error",
                    "message": error_message
                })
        
        # Close connection
        conn.close()
        
        # Print summary
        summary = f"INGESTION SUMMARY: {success_count} succeeded, {error_count} failed, {len(matching_files) - len(new_files)} skipped (already ingested)"
        logging.info("=" * 80)
        logging.info(summary)
        logging.info("=" * 80)
        
        if error_count > 0:
            return {
                "status": "partial",
                "succeeded": success_count,
                "failed": error_count,
                "skipped": len(matching_files) - len(new_files),
                "results": results
            }
        else:
            return {
                "status": "success",
                "succeeded": success_count,
                "failed": 0,
                "skipped": len(matching_files) - len(new_files),
                "results": results
            }
        
    except Exception as e:
        error_message = str(e)
        logging.error(f"Error during ingestion process: {error_message}")
        
        return {
            "status": "error",
            "message": error_message
        }

def main():
    parser = argparse.ArgumentParser(description='Ingest CSV files to SQLite table')
    parser.add_argument('directory_path', help='Path to the directory containing CSV files')
    parser.add_argument('file_prefix', help='Prefix of the CSV files to ingest')
    parser.add_argument('table_name', help='Name of the SQLite table')
    parser.add_argument('--db', default='seo_assessment.db', help='Path to the SQLite database file')
    parser.add_argument('--data-date', help='Batch date for the data_date column (YYYY-MM-DD format)')
    parser.add_argument('--log-level', default='INFO', help='Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)')
    parser.add_argument('--log-file', help='Path to log file (optional)')
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level, args.log_file)
    
    result = ingest_csv_to_sqlite(args.directory_path, args.file_prefix, args.table_name, args.db, args.data_date)
    
    if result["status"] == "success":
        logging.info("CSV ingestion completed successfully")
        sys.exit(0)
    elif result["status"] == "partial":
        logging.warning(f"CSV ingestion partially completed ({result['succeeded']} succeeded, {result['failed']} failed)")
        sys.exit(1)
    elif result["status"] == "skipped":
        logging.info("CSV ingestion skipped (all files already processed)")
        sys.exit(0)
    else:
        logging.error(f"CSV ingestion failed: {result.get('message', 'Unknown error')}")
        sys.exit(1)

if __name__ == "__main__":
    main()