#!/usr/bin/env python3
"""
Snowflake Stage File Download Script

This script downloads all CSV files from the Snowflake stage unload.s.unload
using the same warehouse connection configuration as the load test script.
"""

import os
import sys
from snowflake_connection import connect_to_snowflake, test_connection
import tempfile
from pathlib import Path


def list_stage_files(conn, stage_name="unload.s.unload", file_pattern=None):
    """
    List all files in the specified Snowflake stage
    
    Args:
        conn: Snowflake connection object
        stage_name: Name of the stage to list files from
        file_pattern: Pattern to match files (default: *.csv)
    
    Returns:
        List of file information dictionaries
    """
    try:
        cursor = conn.cursor()
        
        # List files in the stage - apply pattern if provided
        if file_pattern is not None:
            list_query = f"LIST @{stage_name} PATTERN = '{file_pattern}'"
            print(f"📋 Listing files in stage @{stage_name} with pattern '{file_pattern}'...")
        else:
            list_query = f"LIST @{stage_name}"
            print(f"📋 Listing all files in stage @{stage_name} (will filter for CSV files)...")
        
        cursor.execute(list_query)
        results = cursor.fetchall()
        
        files = []
        for row in results:
            # Parse the result - typically contains name, size, md5, last_modified
            file_info = {
                'name': row[0],  # Full path in stage
                'size': row[1],  # File size in bytes
                'md5': row[2],   # MD5 hash
                'last_modified': row[3]  # Last modified timestamp
            }
            
            # Filter for CSV files when no specific pattern is provided
            if file_pattern is None:
                if file_info['name'].lower().endswith('.csv'):
                    files.append(file_info)
            else:
                # If a pattern was specified, include all results (pattern was applied in query)
                files.append(file_info)
        
        cursor.close()
        print(f"✅ Found {len(files)} files in stage")
        
        return files
        
    except Exception as e:
        print(f"❌ Error listing stage files: {e}")
        return []


def download_file_from_stage(conn, stage_name, file_name, local_directory="./downloads"):
    """
    Download a single file from Snowflake stage to local directory
    
    Args:
        conn: Snowflake connection object
        stage_name: Name of the stage
        file_name: Name/path of the file in the stage
        local_directory: Local directory to save the file
    
    Returns:
        True if successful, False otherwise
    """
    try:
        cursor = conn.cursor()
        
        # Create local directory if it doesn't exist
        os.makedirs(local_directory, exist_ok=True)
        
        # Extract just the filename from the full stage path
        local_filename = os.path.basename(file_name)
        local_filepath = os.path.join(local_directory, local_filename)
        
        # Extract the actual filename from the path by splitting on "/" and taking the last part
        # For example: "unload/orders.csv_0_7_1.csv" -> "orders.csv_0_7_1.csv"
        clean_file_name = file_name.split("/")[-1]
        
        # Use GET command to download the file
        get_query = f"GET @{stage_name}/{clean_file_name} file://{os.path.abspath(local_directory)}/"
        
        print(f"⬇️  Downloading {file_name} to {local_filepath}...")
        print(f"    GET command: {get_query}")
        cursor.execute(get_query)
        
        cursor.close()
        
        # Check if file was downloaded successfully
        if os.path.exists(local_filepath):
            file_size = os.path.getsize(local_filepath)
            print(f"✅ Downloaded {local_filename} ({file_size} bytes)")
            return True
        else:
            print(f"❌ File {local_filename} was not downloaded")
            return False
            
    except Exception as e:
        print(f"❌ Error downloading {file_name}: {e}")
        return False


def download_all_csv_files(conn, stage_name="unload.s.unload", local_directory="./downloads"):
    """
    Download all CSV files from the specified Snowflake stage
    
    Args:
        conn: Snowflake connection object
        stage_name: Name of the stage to download from
        local_directory: Local directory to save files
    
    Returns:
        Dictionary with download statistics
    """
    print(f"\n🚀 Starting download of all CSV files from @{stage_name}")
    print(f"📁 Local directory: {os.path.abspath(local_directory)}")
    
    # List all CSV files in the stage
    files = list_stage_files(conn, stage_name)
    
    if not files:
        print("❌ No CSV files found in stage")
        return {'total': 0, 'successful': 0, 'failed': 0}
    
    # Download each file
    successful_downloads = 0
    failed_downloads = 0
    total_size = 0
    
    for file_info in files:
        file_name = file_info['name']
        file_size = file_info['size']
        
        if download_file_from_stage(conn, stage_name, file_name, local_directory):
            successful_downloads += 1
            total_size += file_size
        else:
            failed_downloads += 1
    
    # Print summary
    print(f"\n📊 Download Summary:")
    print(f"   Total files found: {len(files)}")
    print(f"   Successfully downloaded: {successful_downloads}")
    print(f"   Failed downloads: {failed_downloads}")
    print(f"   Total size downloaded: {total_size:,} bytes ({total_size / (1024*1024):.2f} MB)")
    
    return {
        'total': len(files),
        'successful': successful_downloads,
        'failed': failed_downloads,
        'total_size': total_size
    }


def main():
    """
    Main function to download all CSV files from the Snowflake stage
    """
    config_path = "/Users/srramaswamy/.snowsql/config"
    stage_name = "unload.s.unload"  # MODIFY THIS TO CHANGE TARGET STAGE
    local_directory = "./downloads"  # MODIFY THIS TO CHANGE DOWNLOAD DIRECTORY
    
    print("🔗 Connecting to Snowflake...")
    
    # Test basic connection first
    conn = connect_to_snowflake("my_conn", config_path)
    if not conn:
        print("❌ Failed to establish connection.")
        sys.exit(1)
    
    if not test_connection(conn):
        print("❌ Connection test failed.")
        conn.close()
        sys.exit(1)
    
    print("\n🎉 Connection is working properly!")
    
    try:
        # Set warehouse (same as load test)
        cursor = conn.cursor()
        cursor.execute("USE WAREHOUSE loadtest")
        cursor.close()
        print("✅ Using warehouse: loadtest")
        
        # Download all CSV files
        stats = download_all_csv_files(conn, stage_name, local_directory)
        
        if stats['successful'] > 0:
            print(f"\n🎉 Download completed successfully!")
            print(f"   Files are available in: {os.path.abspath(local_directory)}")
        else:
            print(f"\n⚠️  No files were downloaded successfully")
            
    except Exception as e:
        print(f"❌ Error during download process: {e}")
    finally:
        conn.close()
        print("🔌 Connection closed")


if __name__ == "__main__":
    main() 