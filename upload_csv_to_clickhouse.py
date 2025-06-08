#!/usr/bin/env python3
"""
CSV to ClickHouse Upload Script

This script uploads lineitem and orders CSV files to ClickHouse tables:
- lineitem CSV files → tpc_h.lineitem table
- orders CSV files → tpc_h.orders table
"""

import os
import sys
import csv
import glob
from datetime import datetime, date
from pathlib import Path
from clickhouse_connection import connect_to_clickhouse, test_connection


def get_lineitem_schema():
    """
    Returns the ClickHouse table schema for lineitem
    """
    return """
    CREATE TABLE IF NOT EXISTS tpc_h.lineitem (
        l_orderkey UInt64,
        l_partkey UInt64,
        l_suppkey UInt64,
        l_linenumber UInt32,
        l_quantity Decimal(15,2),
        l_extendedprice Decimal(15,2),
        l_discount Decimal(15,2),
        l_tax Decimal(15,2),
        l_returnflag String,
        l_linestatus String,
        l_shipdate Date,
        l_commitdate Date,
        l_receiptdate Date,
        l_shipinstruct String,
        l_shipmode String,
        l_comment String
    ) ENGINE = MergeTree()
    ORDER BY (l_orderkey, l_linenumber)
    """


def get_orders_schema():
    """
    Returns the ClickHouse table schema for orders
    """
    return """
    CREATE TABLE IF NOT EXISTS tpc_h.orders (
        o_orderkey UInt64,
        o_custkey UInt64,
        o_orderstatus String,
        o_totalprice Decimal(15,2),
        o_orderdate Date,
        o_orderpriority String,
        o_clerk String,
        o_shippriority UInt32,
        o_comment String
    ) ENGINE = MergeTree()
    ORDER BY o_orderkey
    """


def create_database_and_tables(client):
    """
    Create the tpc_h database and required tables if they don't exist
    
    Args:
        client: ClickHouse client object
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        print("🏗️  Creating database and tables...")
        
        # Create database
        client.command("CREATE DATABASE IF NOT EXISTS tpc_h")
        print("   ✅ Database 'tpc_h' created/verified")
        
        # Create lineitem table
        client.command(get_lineitem_schema())
        print("   ✅ Table 'tpc_h.lineitem' created/verified")
        
        # Create orders table
        client.command(get_orders_schema())
        print("   ✅ Table 'tpc_h.orders' created/verified")
        
        return True
        
    except Exception as e:
        print(f"❌ Error creating database/tables: {e}")
        return False


def find_csv_files(pattern):
    """
    Find CSV files matching the pattern
    
    Args:
        pattern: File pattern to match
    
    Returns:
        List of file paths
    """
    files = glob.glob(pattern, recursive=True)
    files.extend(glob.glob(f"**/{pattern}", recursive=True))
    files.extend(glob.glob(f"downloads/{pattern}", recursive=True))
    
    # Remove duplicates and sort
    files = sorted(list(set(files)))
    return files


def validate_csv_headers(file_path, expected_headers):
    """
    Validate that CSV file has the expected headers
    
    Args:
        file_path: Path to CSV file
        expected_headers: List of expected header names
    
    Returns:
        tuple: (is_valid, actual_headers)
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader, [])
            
            # Clean headers (strip whitespace, lowercase)
            clean_headers = [h.strip().lower() for h in headers]
            expected_clean = [h.lower() for h in expected_headers]
            
            is_valid = clean_headers == expected_clean
            return is_valid, headers
            
    except Exception as e:
        print(f"❌ Error reading headers from {file_path}: {e}")
        return False, []


def convert_lineitem_row(row):
    """
    Convert a lineitem CSV row to proper data types for ClickHouse
    
    Args:
        row: List of string values from CSV
    
    Returns:
        List of properly typed values
    """
    try:
        converted_row = []
        for i, value in enumerate(row):
            if value == '' or value is None:
                converted_row.append(None)
                continue
                
            # Convert based on column position
            if i in [0, 1, 2]:  # l_orderkey, l_partkey, l_suppkey (UInt64)
                converted_row.append(int(value))
            elif i == 3:  # l_linenumber (UInt32)
                converted_row.append(int(value))
            elif i in [4, 5, 6, 7]:  # quantities, prices, discount, tax (Decimal)
                converted_row.append(float(value))
            elif i in [8, 9]:  # l_returnflag, l_linestatus (String)
                converted_row.append(str(value))
            elif i in [10, 11, 12]:  # dates (Date)
                if value and value.strip():
                    # Parse date string (format: YYYY-MM-DD)
                    date_obj = datetime.strptime(value.strip(), '%Y-%m-%d').date()
                    converted_row.append(date_obj)
                else:
                    converted_row.append(None)
            else:  # l_shipinstruct, l_shipmode, l_comment (String)
                converted_row.append(str(value))
        
        return converted_row
    except Exception as e:
        print(f"      Error converting row: {e}, Row data: {row[:3]}...")
        return None


def convert_orders_row(row):
    """
    Convert an orders CSV row to proper data types for ClickHouse
    
    Args:
        row: List of string values from CSV
    
    Returns:
        List of properly typed values
    """
    try:
        converted_row = []
        for i, value in enumerate(row):
            if value == '' or value is None:
                converted_row.append(None)
                continue
                
            # Convert based on column position
            if i in [0, 1]:  # o_orderkey, o_custkey (UInt64)
                converted_row.append(int(value))
            elif i == 2:  # o_orderstatus (String)
                converted_row.append(str(value))
            elif i == 3:  # o_totalprice (Decimal)
                converted_row.append(float(value))
            elif i == 4:  # o_orderdate (Date)
                if value and value.strip():
                    # Parse date string (format: YYYY-MM-DD)
                    date_obj = datetime.strptime(value.strip(), '%Y-%m-%d').date()
                    converted_row.append(date_obj)
                else:
                    converted_row.append(None)
            elif i == 5:  # o_orderpriority (String)
                converted_row.append(str(value))
            elif i == 6:  # o_clerk (String)
                converted_row.append(str(value))
            elif i == 7:  # o_shippriority (UInt32)
                converted_row.append(int(value))
            else:  # o_comment (String)
                converted_row.append(str(value))
        
        return converted_row
    except Exception as e:
        print(f"      Error converting row: {e}, Row data: {row[:3]}...")
        return None


def upload_csv_file(client, file_path, table_name, expected_headers, converter_func=None):
    """
    Upload a single CSV file to ClickHouse table
    
    Args:
        client: ClickHouse client object
        file_path: Path to CSV file
        table_name: Target table name
        expected_headers: List of expected column names
        converter_func: Function to convert row data types
    
    Returns:
        tuple: (success, rows_inserted)
    """
    try:
        print(f"📤 Uploading {file_path} to {table_name}...")
        
        # Validate headers
        is_valid, actual_headers = validate_csv_headers(file_path, expected_headers)
        if not is_valid:
            print(f"   ❌ Header validation failed")
            print(f"      Expected: {expected_headers}")
            print(f"      Found: {actual_headers}")
            return False, 0
        
        # Read CSV data
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)  # Skip header row
            
            # Convert rows to list for bulk insert
            data_rows = []
            skipped_rows = 0
            for row_num, row in enumerate(reader, 1):
                if len(row) != len(expected_headers):
                    print(f"   ⚠️  Row {row_num} has {len(row)} columns, expected {len(expected_headers)}")
                    skipped_rows += 1
                    continue
                
                # Convert data types if converter function provided
                if converter_func:
                    converted_row = converter_func(row)
                    if converted_row is None:
                        skipped_rows += 1
                        continue
                    data_rows.append(converted_row)
                else:
                    data_rows.append(row)
                
                # Progress indicator for large files
                if row_num % 10000 == 0:
                    print(f"      Read {row_num:,} rows...")
        
        if not data_rows:
            print(f"   ⚠️  No valid data rows found")
            return False, 0
        
        print(f"   📊 Read {len(data_rows):,} data rows")
        if skipped_rows > 0:
            print(f"   ⚠️  Skipped {skipped_rows} rows due to conversion errors")
        
        # Insert data using clickhouse-connect
        client.insert(table_name, data_rows, column_names=expected_headers)
        
        print(f"   ✅ Successfully uploaded {len(data_rows):,} rows")
        return True, len(data_rows)
        
    except Exception as e:
        print(f"   ❌ Error uploading {file_path}: {e}")
        return False, 0


def upload_lineitem_files(client):
    """
    Upload all lineitem CSV files to tpc_h.lineitem table
    
    Args:
        client: ClickHouse client object
    
    Returns:
        tuple: (files_processed, total_rows_inserted)
    """
    print(f"\n🔍 Finding lineitem CSV files...")
    
    lineitem_files = find_csv_files("*lineitem*.csv")
    if not lineitem_files:
        print("❌ No lineitem CSV files found")
        return 0, 0
    
    print(f"📋 Found {len(lineitem_files)} lineitem files:")
    for file in lineitem_files:
        print(f"   - {file}")
    
    expected_headers = [
        'l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 
        'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus',
        'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 
        'l_shipmode', 'l_comment'
    ]
    
    files_processed = 0
    total_rows = 0
    
    for file_path in lineitem_files:
        success, rows = upload_csv_file(client, file_path, "tpc_h.lineitem", expected_headers, convert_lineitem_row)
        if success:
            files_processed += 1
            total_rows += rows
    
    return files_processed, total_rows


def upload_orders_files(client):
    """
    Upload all orders CSV files to tpc_h.orders table
    
    Args:
        client: ClickHouse client object
    
    Returns:
        tuple: (files_processed, total_rows_inserted)
    """
    print(f"\n🔍 Finding orders CSV files...")
    
    orders_files = find_csv_files("*order*.csv")
    if not orders_files:
        print("❌ No orders CSV files found")
        return 0, 0
    
    print(f"📋 Found {len(orders_files)} orders files:")
    for file in orders_files:
        print(f"   - {file}")
    
    expected_headers = [
        'o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice',
        'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment'
    ]
    
    files_processed = 0
    total_rows = 0
    
    for file_path in orders_files:
        success, rows = upload_csv_file(client, file_path, "tpc_h.orders", expected_headers, convert_orders_row)
        if success:
            files_processed += 1
            total_rows += rows
    
    return files_processed, total_rows


def show_table_stats(client):
    """
    Show statistics for the uploaded tables
    
    Args:
        client: ClickHouse client object
    """
    try:
        print(f"\n📊 Table Statistics:")
        
        # Lineitem stats
        result = client.query("SELECT COUNT(*) FROM tpc_h.lineitem")
        lineitem_count = result.result_rows[0][0]
        print(f"   tpc_h.lineitem: {lineitem_count:,} rows")
        
        # Orders stats
        result = client.query("SELECT COUNT(*) FROM tpc_h.orders")
        orders_count = result.result_rows[0][0]
        print(f"   tpc_h.orders: {orders_count:,} rows")
        
    except Exception as e:
        print(f"❌ Error getting table stats: {e}")


def main():
    """
    Main function to upload CSV files to ClickHouse
    """
    print("🚀 CSV to ClickHouse Upload Tool")
    print("=" * 50)
    
    # Connect to ClickHouse
    client = connect_to_clickhouse()
    if not client:
        print("❌ Failed to connect to ClickHouse")
        sys.exit(1)
    
    # Create database and tables
    if not create_database_and_tables(client):
        print("❌ Failed to create database/tables")
        sys.exit(1)
    
    # Upload lineitem files
    print(f"\n{'='*50}")
    print("📤 UPLOADING LINEITEM FILES")
    print(f"{'='*50}")
    
    lineitem_files, lineitem_rows = upload_lineitem_files(client)
    
    # Upload orders files
    print(f"\n{'='*50}")
    print("📤 UPLOADING ORDERS FILES")
    print(f"{'='*50}")
    
    orders_files, orders_rows = upload_orders_files(client)
    
    # Show final statistics
    show_table_stats(client)
    
    # Summary
    print(f"\n🎉 Upload Summary:")
    print(f"   Lineitem files processed: {lineitem_files}")
    print(f"   Lineitem rows inserted: {lineitem_rows:,}")
    print(f"   Orders files processed: {orders_files}")
    print(f"   Orders rows inserted: {orders_rows:,}")
    print(f"   Total rows inserted: {(lineitem_rows + orders_rows):,}")
    
    # Close connection
    client.close()
    print("🔌 Connection closed")


if __name__ == "__main__":
    main() 