#!/usr/bin/env python3
"""
LineItem CSV Header Fixer

This script ensures all lineitem CSV files have the correct header row
with the proper column names.
"""

import os
import csv
import glob
from pathlib import Path
import shutil
from datetime import datetime


def get_lineitem_headers():
    """
    Returns the correct header row for lineitem CSV files
    """
    return [
        'l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 
        'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus',
        'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 
        'l_shipmode', 'l_comment'
    ]


def find_lineitem_files(directory=".", pattern="*lineitem*.csv"):
    """
    Find all lineitem CSV files in the specified directory
    
    Args:
        directory: Directory to search in
        pattern: File pattern to match
    
    Returns:
        List of file paths
    """
    search_pattern = os.path.join(directory, pattern)
    files = glob.glob(search_pattern, recursive=True)
    
    # Also search in subdirectories
    recursive_pattern = os.path.join(directory, "**", pattern)
    files.extend(glob.glob(recursive_pattern, recursive=True))
    
    # Remove duplicates and sort
    files = sorted(list(set(files)))
    
    print(f"🔍 Found {len(files)} lineitem CSV files:")
    for file in files:
        print(f"   - {file}")
    
    return files


def check_header(file_path):
    """
    Check if a CSV file has the correct header
    
    Args:
        file_path: Path to the CSV file
    
    Returns:
        tuple: (has_header, current_header, needs_fix)
    """
    expected_header = get_lineitem_headers()
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # Try to detect if first row is header
            sample = f.read(1024)
            f.seek(0)
            
            # Check if first line contains column names (has letters)
            first_line = f.readline().strip()
            
            if not first_line:
                return False, [], True
            
            # Parse the first row
            f.seek(0)
            reader = csv.reader(f)
            first_row = next(reader, [])
            
            # Check if it looks like a header (contains letters)
            has_header = any(any(c.isalpha() for c in cell) for cell in first_row)
            
            if has_header:
                # Clean up the header (strip whitespace, lowercase)
                current_header = [cell.strip().lower() for cell in first_row]
                needs_fix = current_header != expected_header
            else:
                current_header = []
                needs_fix = True
            
            return has_header, current_header, needs_fix
            
    except Exception as e:
        print(f"❌ Error reading {file_path}: {e}")
        return False, [], True


def fix_csv_header(file_path, backup=True):
    """
    Fix the header of a CSV file
    
    Args:
        file_path: Path to the CSV file
        backup: Whether to create a backup of the original file
    
    Returns:
        bool: True if successful, False otherwise
    """
    expected_header = get_lineitem_headers()
    
    try:
        # Create backup if requested
        if backup:
            backup_path = f"{file_path}.backup.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            shutil.copy2(file_path, backup_path)
            print(f"   📋 Backup created: {backup_path}")
        
        # Read all data
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            all_rows = list(reader)
        
        if not all_rows:
            print(f"   ⚠️  File is empty")
            return False
        
        # Check if first row is a header
        has_header, current_header, needs_fix = check_header(file_path)
        
        # Prepare new content
        if has_header and not needs_fix:
            print(f"   ✅ Header is already correct")
            return True
        elif has_header:
            # Replace existing header
            new_rows = [expected_header] + all_rows[1:]
            print(f"   🔄 Replacing existing header")
        else:
            # Add header to file without one
            new_rows = [expected_header] + all_rows
            print(f"   ➕ Adding header to file")
        
        # Write updated content
        with open(file_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(new_rows)
        
        print(f"   ✅ Header fixed successfully")
        return True
        
    except Exception as e:
        print(f"   ❌ Error fixing {file_path}: {e}")
        return False


def validate_csv_structure(file_path):
    """
    Validate that the CSV has the correct number of columns
    
    Args:
        file_path: Path to the CSV file
    
    Returns:
        tuple: (is_valid, num_columns, sample_rows_checked)
    """
    expected_columns = len(get_lineitem_headers())
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            
            # Skip header
            header = next(reader, [])
            if len(header) != expected_columns:
                return False, len(header), 0
            
            # Check first few data rows
            rows_checked = 0
            for i, row in enumerate(reader):
                if i >= 5:  # Check first 5 data rows
                    break
                if len(row) != expected_columns:
                    return False, len(row), rows_checked + 1
                rows_checked += 1
            
            return True, expected_columns, rows_checked
            
    except Exception as e:
        print(f"❌ Error validating {file_path}: {e}")
        return False, 0, 0


def main():
    """
    Main function to fix headers in all lineitem CSV files
    """
    print("🚀 LineItem CSV Header Fixer")
    print("=" * 50)
    
    # Configuration
    search_directory = "."  # Current directory
    create_backups = True
    
    # Find all lineitem CSV files
    lineitem_files = find_lineitem_files(search_directory)
    
    if not lineitem_files:
        print("❌ No lineitem CSV files found")
        return
    
    print(f"\n📝 Expected header:")
    expected_header = get_lineitem_headers()
    print(f"   {', '.join(expected_header)}")
    
    print(f"\n🔧 Processing {len(lineitem_files)} files...")
    
    # Process each file
    fixed_count = 0
    error_count = 0
    
    for i, file_path in enumerate(lineitem_files, 1):
        print(f"\n📄 Processing file {i}/{len(lineitem_files)}: {file_path}")
        
        # Check current header
        has_header, current_header, needs_fix = check_header(file_path)
        
        if not needs_fix:
            print(f"   ✅ Header is already correct")
        else:
            if has_header:
                print(f"   🔄 Current header: {', '.join(current_header)}")
            else:
                print(f"   ❌ No header found")
            
            # Fix the header
            if fix_csv_header(file_path, create_backups):
                fixed_count += 1
            else:
                error_count += 1
        
        # Validate structure
        is_valid, num_cols, rows_checked = validate_csv_structure(file_path)
        if is_valid:
            print(f"   ✅ Structure valid ({num_cols} columns, {rows_checked} rows checked)")
        else:
            print(f"   ⚠️  Structure issue: found {num_cols} columns, expected {len(expected_header)}")
    
    # Summary
    print(f"\n📊 Summary:")
    print(f"   Total files processed: {len(lineitem_files)}")
    print(f"   Files fixed: {fixed_count}")
    print(f"   Errors: {error_count}")
    print(f"   Already correct: {len(lineitem_files) - fixed_count - error_count}")
    
    if create_backups and fixed_count > 0:
        print(f"\n💾 Backup files were created with timestamp suffixes")
        print(f"   You can delete them after verifying the results")


if __name__ == "__main__":
    main() 