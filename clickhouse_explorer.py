#!/usr/bin/env python3
"""
ClickHouse Database Explorer

This script connects to ClickHouse Cloud and explores the database structure,
showing tables, row counts, sizes, and basic information.
"""

import sys
from clickhouse_connection import connect_to_clickhouse, test_connection


def get_databases(client):
    """
    Get list of databases
    
    Args:
        client: ClickHouse client object
    
    Returns:
        List of database names
    """
    try:
        result = client.query("SHOW DATABASES")
        databases = [row[0] for row in result.result_rows]
        return databases
    except Exception as e:
        print(f"❌ Error getting databases: {e}")
        return []


def get_tables_in_database(client, database):
    """
    Get list of tables in a database
    
    Args:
        client: ClickHouse client object
        database: Database name
    
    Returns:
        List of table names
    """
    try:
        result = client.query(f"SHOW TABLES FROM {database}")
        tables = [row[0] for row in result.result_rows]
        return tables
    except Exception as e:
        print(f"❌ Error getting tables from {database}: {e}")
        return []


def get_table_info(client, database, table):
    """
    Get detailed information about a table
    
    Args:
        client: ClickHouse client object
        database: Database name
        table: Table name
    
    Returns:
        Dictionary with table information
    """
    table_info = {
        'name': table,
        'rows': 0,
        'size_bytes': 0,
        'columns': []
    }
    
    try:
        # Get row count
        result = client.query(f"SELECT COUNT(*) FROM {database}.{table}")
        table_info['rows'] = result.result_rows[0][0]
        
        # Get table size (approximate)
        result = client.query(f"""
            SELECT 
                sum(data_compressed_bytes) as size_bytes
            FROM system.parts 
            WHERE database = '{database}' AND table = '{table}' AND active = 1
        """)
        if result.result_rows and result.result_rows[0][0]:
            table_info['size_bytes'] = result.result_rows[0][0]
        
        # Get column information
        result = client.query(f"DESCRIBE TABLE {database}.{table}")
        for row in result.result_rows:
            column_info = {
                'name': row[0],
                'type': row[1],
                'default_type': row[2] if len(row) > 2 else '',
                'default_expression': row[3] if len(row) > 3 else ''
            }
            table_info['columns'].append(column_info)
            
    except Exception as e:
        print(f"❌ Error getting info for {database}.{table}: {e}")
    
    return table_info


def format_bytes(bytes_val):
    """
    Format bytes to human readable format
    
    Args:
        bytes_val: Number of bytes
    
    Returns:
        Formatted string
    """
    if bytes_val == 0:
        return "0 B"
    
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_val < 1024.0:
            return f"{bytes_val:.1f} {unit}"
        bytes_val /= 1024.0
    
    return f"{bytes_val:.1f} PB"


def explore_database(client, database_name):
    """
    Explore a specific database and show detailed information
    
    Args:
        client: ClickHouse client object
        database_name: Name of database to explore
    """
    print(f"\n🔍 Exploring Database: {database_name}")
    print("=" * 60)
    
    tables = get_tables_in_database(client, database_name)
    
    if not tables:
        print("   No tables found in this database")
        return
    
    print(f"📋 Found {len(tables)} tables:")
    
    total_rows = 0
    total_size = 0
    
    for table in tables:
        table_info = get_table_info(client, database_name, table)
        
        print(f"\n📊 Table: {table}")
        print(f"   Rows: {table_info['rows']:,}")
        print(f"   Size: {format_bytes(table_info['size_bytes'])}")
        print(f"   Columns: {len(table_info['columns'])}")
        
        # Show first few columns
        if table_info['columns']:
            print("   Column Details:")
            for i, col in enumerate(table_info['columns'][:5]):  # Show first 5 columns
                print(f"      {i+1}. {col['name']} ({col['type']})")
            if len(table_info['columns']) > 5:
                print(f"      ... and {len(table_info['columns']) - 5} more columns")
        
        total_rows += table_info['rows']
        total_size += table_info['size_bytes']
    
    print(f"\n📈 Database Summary:")
    print(f"   Total Tables: {len(tables)}")
    print(f"   Total Rows: {total_rows:,}")
    print(f"   Total Size: {format_bytes(total_size)}")


def main():
    """
    Main function to explore ClickHouse databases
    """
    print("🚀 ClickHouse Database Explorer")
    print("=" * 50)
    
    # Connect to ClickHouse
    client = connect_to_clickhouse()
    if not client:
        print("❌ Failed to connect to ClickHouse")
        sys.exit(1)
    
    # Get list of databases
    print(f"\n🗂️  Getting list of databases...")
    databases = get_databases(client)
    
    if not databases:
        print("❌ No databases found")
        sys.exit(1)
    
    print(f"📋 Found {len(databases)} databases:")
    for db in databases:
        print(f"   - {db}")
    
    # Focus on tpc_h database if it exists
    if 'tpc_h' in databases:
        explore_database(client, 'tpc_h')
    else:
        print(f"\n⚠️  tpc_h database not found")
        
        # Explore the first non-system database
        user_databases = [db for db in databases if not db.startswith('system') and db not in ['default', 'information_schema']]
        if user_databases:
            explore_database(client, user_databases[0])
    
    # Close connection
    client.close()
    print(f"\n🔌 Connection closed")


if __name__ == "__main__":
    main() 