#!/usr/bin/env python3
"""
Snowflake Database Load Test Script

This script runs load tests against Snowflake using the shared load testing framework.
It tests the SNOWFLAKE_SAMPLE_DATA.TPCH_SF1 tables with configurable QPS loads.
"""

import sys
import snowflake.connector
from snowflake_connection import connect_to_snowflake, test_connection, get_connection_params
from shared_loadtest import run_load_test_suite, generate_reports


def create_snowflake_connection(connection_params):
    """
    Create a Snowflake connection for worker threads
    
    Args:
        connection_params: Dictionary with connection parameters
    
    Returns:
        Snowflake connection object with cursor configured
    """
    conn = snowflake.connector.connect(
        account=connection_params['account'],
        user=connection_params['user'],
        private_key=connection_params['private_key'],
        authenticator='SNOWFLAKE_JWT'
    )
    
    cursor = conn.cursor()
    cursor.execute("USE WAREHOUSE loadtest")
    
    # Store cursor in connection object for easy access
    conn._cursor = cursor
    return conn


def execute_snowflake_query(conn, table_config, random_key):
    """
    Execute a query against Snowflake using the provided connection
    
    Args:
        conn: Snowflake connection object (with cursor)
        table_config: Dictionary containing table configuration
        random_key: Random key value for WHERE clause
    
    Returns:
        Number of results returned
    """
    query = f"""
    SELECT {table_config['select_column']} 
    FROM {table_config['database']}.{table_config['schema']}.{table_config['table']} 
    WHERE {table_config['key_column']} = {random_key}
    """
    
    cursor = conn._cursor
    cursor.execute(query)
    results = cursor.fetchall()
    return len(results)


def test_snowflake_connection():
    """
    Test basic Snowflake connectivity
    
    Returns:
        bool: True if connection successful, False otherwise
    """
    config_path = "/Users/srramaswamy/.snowsql/config"
    
    conn = connect_to_snowflake("my_conn", config_path)
    if conn:
        success = test_connection(conn)
        conn.close()
        return success
    return False


def main():
    """
    Main function to run Snowflake load tests
    """
    print("🚀 Snowflake Load Test Suite")
    print("=" * 50)
    
    config_path = "/Users/srramaswamy/.snowsql/config"
    
    # Get connection parameters for threading
    conn_params = get_connection_params(config_path, "my_conn")
    if not conn_params:
        print("Failed to get connection parameters.")
        sys.exit(1)
    
    # Configure test parameters
    qps_values = [10, 25, 50, 100, 200, 300]
    warmup_seconds = 60    # Warmup duration 
    run_seconds = 120      # Test duration
    
    # Configure table to test against - MODIFY THIS TO CHANGE TARGET TABLE
    # 
    # Examples for Snowflake SNOWFLAKE_SAMPLE_DATA.TPCH_SF1:
    # For ORDERS table:
    #   database: 'SNOWFLAKE_SAMPLE_DATA', schema: 'TPCH_SF1', table: 'ORDERS', key_column: 'O_ORDERKEY', select_column: 'O_COMMENT', key_range: [1, 6000000] 
    # For LINEITEM table:
    #   database: 'SNOWFLAKE_SAMPLE_DATA', schema: 'TPCH_SF1', table: 'LINEITEM', key_column: 'L_ORDERKEY', select_column: 'L_COMMENT', key_range: [1, 6000000]
    # For CUSTOMER table: 
    #   database: 'SNOWFLAKE_SAMPLE_DATA', schema: 'TPCH_SF1', table: 'CUSTOMER', key_column: 'C_CUSTKEY', select_column: 'C_NAME', key_range: [1, 150000]
    # For PART table:
    #   database: 'SNOWFLAKE_SAMPLE_DATA', schema: 'TPCH_SF1', table: 'PART', key_column: 'P_PARTKEY', select_column: 'P_NAME', key_range: [1, 200000]
    #
    table_config_orders = {
        'database': 'SNOWFLAKE_SAMPLE_DATA',
        'schema': 'TPCH_SF1', 
        'table': 'ORDERS',
        'key_column': 'O_ORDERKEY',
        'select_column': 'O_COMMENT',
        'key_range': [1, 6000000]  # Range for random O_ORDERKEY selection
    }
    
    table_config_lineitem = {
        'database': 'SNOWFLAKE_SAMPLE_DATA',
        'schema': 'TPCH_SF1', 
        'table': 'LINEITEM',
        'key_column': 'L_ORDERKEY',
        'select_column': 'L_COMMENT',
        'key_range': [1, 6000000]  # Range for random L_ORDERKEY selection
    }
    
    # Run tests for both tables
    table_configs = [table_config_lineitem, table_config_orders]
    
    print(f"\n🎯 Load Test Configuration:")
    print(f"   Tables to test: lineitem, orders")
    print(f"   QPS values to test: {qps_values}")
    print(f"   Warmup duration: {warmup_seconds}s")
    print(f"   Test duration: {run_seconds}s")
    print(f"   Total estimated time: {len(table_configs) * len(qps_values) * (warmup_seconds + run_seconds + 10) / 60:.1f} minutes")
    
    all_results = []
    
    # Run tests for each table
    for i, table_config in enumerate(table_configs):
        table_name = table_config['table']
        print(f"\n{'='*70}")
        print(f"🚀 Starting Load Test {i+1}/{len(table_configs)}: {table_name.upper()} Table")
        print(f"{'='*70}")
        print(f"   Target Table: {table_config['database']}.{table_config['schema']}.{table_config['table']}")
        print(f"   Key Column: {table_config['key_column']} (range: {table_config['key_range'][0]}-{table_config['key_range'][1]})")
        print(f"   Select Column: {table_config['select_column']}")
        
        # Run the load test suite using shared framework
        test_results = run_load_test_suite(
            connection_params=conn_params,
            create_connection_func=create_snowflake_connection,
            execute_query_func=execute_snowflake_query,
            test_connection_func=test_snowflake_connection,
            qps_values=qps_values,
            table_config=table_config,
            warmup_seconds=warmup_seconds,
            run_seconds=run_seconds,
            database_name="Snowflake"
        )
        
        if test_results:
            # Generate reports using shared framework
            csv_file, graph_file = generate_reports(test_results, table_config, "Snowflake", "Snowflake XS (8 core, 16GB)")
            
            print(f"\n✅ {table_name.upper()} load test completed!")
            print(f"   CSV report: {csv_file}")
            if graph_file:
                print(f"   Performance graph: {graph_file}")
            
            all_results.append({
                'table': table_name,
                'csv_file': csv_file,
                'graph_file': graph_file,
                'results': test_results
            })
        else:
            print(f"❌ {table_name.upper()} load test failed")
    
    # Final summary
    print(f"\n🎉 All Snowflake load tests completed!")
    print(f"📊 Summary of generated files:")
    for result in all_results:
        print(f"   {result['table'].upper()} Table:")
        print(f"     - CSV: {result['csv_file']}")
        if result['graph_file']:
            print(f"     - Graph: {result['graph_file']}")
    
    if not all_results:
        print("❌ All load tests failed")
        sys.exit(1)


if __name__ == "__main__":
    main() 
    
 