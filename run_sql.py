#!/usr/bin/env python3
"""
SQL Runner for Snowflake

A reusable script that executes SQL files against Snowflake and generates
detailed output files containing both the SQL statements and their results.

Usage:
    python3 run_sql.py <sql_file>
    
Example:
    python3 run_sql.py sql_runner.sql
    
Output:
    Creates <sql_file>.out with SQL statements and results
"""

import sys
import os
import argparse
import re
from datetime import datetime
from snowflake_connection import connect_to_snowflake, test_connection
import pandas as pd


def log_conversation(base_name, user_request, sql_content):
    """
    Log the conversation about this SQL task to a .log file
    
    Args:
        base_name (str): Base name of the SQL file (without .sql)
        user_request (str): The user's request/description of the task
        sql_content (str): The SQL content that was created/executed
    """
    log_file = f"{base_name}.log"
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Check if log file exists to determine if we're starting a new conversation
    is_new_log = not os.path.exists(log_file)
    
    with open(log_file, 'a') as f:
        if is_new_log:
            f.write(f"CONVERSATION LOG: {base_name}\n")
            f.write("=" * 60 + "\n")
            f.write(f"Started: {timestamp}\n")
            f.write("=" * 60 + "\n\n")
        
        f.write(f"[{timestamp}] TASK REQUEST:\n")
        f.write(f"{user_request}\n\n")
        f.write(f"[{timestamp}] SQL CONTENT:\n")
        f.write(f"{sql_content}\n")
        f.write("-" * 40 + "\n\n")


def parse_sql_file(sql_file):
    """
    Parse SQL file and extract individual statements with their labels and descriptions
    
    Args:
        sql_file (str): Path to the SQL file
        
    Returns:
        list: List of dictionaries containing SQL statements and metadata
    """
    try:
        with open(sql_file, 'r') as f:
            content = f.read()
    except FileNotFoundError:
        print(f"❌ Error: SQL file '{sql_file}' not found.")
        return None
    except Exception as e:
        print(f"❌ Error reading SQL file: {e}")
        return None
    
    statements = []
    
    # Split content by statement separators (lines with multiple =)
    sections = re.split(r'\n-- ={10,}\n', content)
    
    for section in sections:
        if not section.strip():
            continue
            
        lines = section.strip().split('\n')
        
        # Extract metadata
        task_line = None
        description = None
        label = None
        sql_lines = []
        
        for line in lines:
            line = line.strip()
            if line.startswith('-- TASK_'):
                task_line = line
            elif line.startswith('-- Description:'):
                description = line.replace('-- Description:', '').strip()
            elif line.startswith('-- Label:'):
                label = line.replace('-- Label:', '').strip()
            elif not line.startswith('--') and line:
                sql_lines.append(line)
        
        # Combine SQL lines
        sql_statement = '\n'.join(sql_lines).strip()
        
        if sql_statement and sql_statement != 'End of SQL Runner File':
            statements.append({
                'task': task_line or 'Unknown Task',
                'description': description or 'No description',
                'label': label or 'unlabeled',
                'sql': sql_statement
            })
    
    return statements


def execute_sql_statement(conn, statement_info):
    """
    Execute a single SQL statement and return results
    
    Args:
        conn: Snowflake connection object
        statement_info (dict): Dictionary containing SQL statement and metadata
        
    Returns:
        dict: Results including success status, data, and execution time
    """
    start_time = datetime.now()
    
    try:
        cursor = conn.cursor()
        cursor.execute(statement_info['sql'])
        
        # Get results
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        data = cursor.fetchall() if cursor.description else []
        
        cursor.close()
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        return {
            'success': True,
            'columns': columns,
            'data': data,
            'row_count': len(data),
            'execution_time': execution_time,
            'error': None
        }
        
    except Exception as e:
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        return {
            'success': False,
            'columns': [],
            'data': [],
            'row_count': 0,
            'execution_time': execution_time,
            'error': str(e)
        }


def format_results_for_output(statement_info, results):
    """
    Format SQL statement and results for output file
    
    Args:
        statement_info (dict): Statement metadata
        results (dict): Execution results
        
    Returns:
        str: Formatted output string
    """
    output = []
    output.append("=" * 80)
    output.append(f"TASK: {statement_info['task']}")
    output.append(f"LABEL: {statement_info['label']}")
    output.append(f"DESCRIPTION: {statement_info['description']}")
    output.append("=" * 80)
    output.append("")
    output.append("SQL STATEMENT:")
    output.append("-" * 40)
    output.append(statement_info['sql'])
    output.append("")
    output.append("EXECUTION RESULTS:")
    output.append("-" * 40)
    
    if results['success']:
        output.append(f"✅ SUCCESS")
        output.append(f"Execution Time: {results['execution_time']:.3f} seconds")
        output.append(f"Rows Returned: {results['row_count']}")
        output.append("")
        
        if results['data']:
            # Create DataFrame for nice formatting
            df = pd.DataFrame(results['data'], columns=results['columns'])
            output.append("RESULTS:")
            output.append(df.to_string(index=False))
        else:
            output.append("No data returned.")
    else:
        output.append(f"❌ ERROR")
        output.append(f"Execution Time: {results['execution_time']:.3f} seconds")
        output.append(f"Error Message: {results['error']}")
    
    output.append("")
    output.append("")
    
    return "\n".join(output)


def main():
    """
    Main function to execute SQL file and generate output
    """
    parser = argparse.ArgumentParser(
        description="Execute SQL files against Snowflake and generate output files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 run_sql.py sql_runner.sql
  python3 run_sql.py analysis.sql
  
Output:
  Creates <sql_file>.out with detailed results
        """
    )
    
    parser.add_argument('sql_file', help='Path to the SQL file to execute')
    parser.add_argument('--config', default="/Users/srramaswamy/.snowsql/config",
                       help='Path to SnowSQL config file')
    parser.add_argument('--connection', default="my_conn",
                       help='Connection name in SnowSQL config')
    parser.add_argument('--log-request', type=str,
                       help='Log a conversation request about this task')
    
    args = parser.parse_args()
    
    # Validate SQL file exists
    if not os.path.exists(args.sql_file):
        print(f"❌ Error: SQL file '{args.sql_file}' not found.")
        sys.exit(1)
    
    print(f"🚀 SQL Runner Starting")
    print(f"📁 SQL File: {args.sql_file}")
    print("=" * 50)
    
    # Parse SQL file
    print("📋 Parsing SQL file...")
    statements = parse_sql_file(args.sql_file)
    
    if not statements:
        print("❌ No valid SQL statements found in file.")
        sys.exit(1)
    
    print(f"✅ Found {len(statements)} SQL statements")
    
    # Connect to Snowflake
    print("\n🔗 Connecting to Snowflake...")
    conn = connect_to_snowflake(args.connection, args.config)
    
    if not conn:
        print("❌ Failed to connect to Snowflake.")
        sys.exit(1)
    
    if not test_connection(conn):
        print("❌ Connection test failed.")
        conn.close()
        sys.exit(1)
    
    # Generate output file name (strip .sql suffix if present)
    base_name = args.sql_file
    if base_name.endswith('.sql'):
        base_name = base_name[:-4]  # Remove .sql suffix
    output_file = f"{base_name}.out"
    
    # Log conversation if requested
    if args.log_request:
        try:
            with open(args.sql_file, 'r') as f:
                sql_content = f.read()
            log_conversation(base_name, args.log_request, sql_content)
            print(f"📝 Logged conversation to: {base_name}.log")
        except Exception as e:
            print(f"⚠️  Warning: Could not log conversation: {e}")
    
    # Execute statements and generate output
    print(f"\n⚡ Executing SQL statements...")
    print(f"📝 Output file: {output_file}")
    
    with open(output_file, 'w') as f:
        # Write header
        f.write(f"SQL RUNNER OUTPUT\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"SQL File: {args.sql_file}\n")
        f.write(f"Statements Executed: {len(statements)}\n")
        f.write("=" * 80 + "\n\n")
        
        # Execute each statement
        for i, statement_info in enumerate(statements, 1):
            print(f"  {i}/{len(statements)}: {statement_info['label']}")
            
            results = execute_sql_statement(conn, statement_info)
            formatted_output = format_results_for_output(statement_info, results)
            f.write(formatted_output)
            
            if results['success']:
                print(f"    ✅ Success - {results['row_count']} rows in {results['execution_time']:.3f}s")
            else:
                print(f"    ❌ Error - {results['error']}")
    
    # Close connection
    conn.close()
    
    print(f"\n🎉 SQL Runner completed!")
    print(f"📊 Results saved to: {output_file}")


if __name__ == "__main__":
    main() 