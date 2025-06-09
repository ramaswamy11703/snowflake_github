#!/usr/bin/env python3
"""
ClickHouse Database Load Test Script

This script runs load tests against ClickHouse Cloud using the shared load testing framework.
It tests the tpc_h database tables (lineitem, orders) with configurable QPS loads.
"""

import sys
from clickhouse_connection import connect_to_clickhouse, test_connection
from shared_loadtest import run_load_test_suite, generate_reports


def create_clickhouse_connection(connection_params):
    """
    Create a ClickHouse connection for worker threads
    
    Args:
        connection_params: Dictionary with connection parameters
    
    Returns:
        ClickHouse client object
    """
    return connect_to_clickhouse(
        api_user=connection_params.get('api_user', 'default'),
        api_user_password=connection_params.get('api_user_password'),
        password_file=connection_params.get('password_file', '~/.snowsql/clickhouse')
    )


def execute_clickhouse_query(client, table_config, random_key):
    """
    Execute a query against ClickHouse using the provided client
    
    Args:
        client: ClickHouse client object
        table_config: Dictionary containing table configuration
        random_key: Random key value for WHERE clause
    
    Returns:
        Number of results returned
    """
    query = f"""
    SELECT {table_config['select_column']} 
    FROM {table_config['database']}.{table_config['table']} 
    WHERE {table_config['key_column']} = {random_key}
    """
    
    result = client.query(query)
    return len(result.result_rows)


def test_clickhouse_connection():
    """
    Test basic ClickHouse connectivity
    
    Returns:
        bool: True if connection successful, False otherwise
    """
    client = connect_to_clickhouse()
    if client:
        success = test_connection(client)
        client.close()
        return success
    return False


def main():
    """
    Main function to run ClickHouse load tests
    """
    print("🚀 ClickHouse Load Test Suite")
    print("=" * 50)
    
    # Configure test parameters
    qps_values = [10, 25, 50, 100, 200, 300]
    warmup_seconds = 60    # Warmup duration 
    run_seconds = 120      # Test duration
    
    # Configure table to test against - MODIFY THIS TO CHANGE TARGET TABLE
    # 
    # Examples for ClickHouse tpc_h database:
    # For LINEITEM table:
    #   database: 'tpc_h', table: 'lineitem', key_column: 'l_orderkey', select_column: 'l_comment', key_range: [1, 6000000]
    # For ORDERS table:
    #   database: 'tpc_h', table: 'orders', key_column: 'o_orderkey', select_column: 'o_comment', key_range: [1, 6000000]
    #
    table_config_lineitem = {
        'database': 'tpc_h',
        'table': 'lineitem',
        'key_column': 'l_orderkey',
        'select_column': 'l_comment',
        'key_range': [1, 6000000]  # Range for random l_orderkey selection
    }
    
    table_config_orders = {
        'database': 'tpc_h',
        'table': 'orders',
        'key_column': 'o_orderkey',
        'select_column': 'o_comment',
        'key_range': [1, 6000000]  # Range for random o_orderkey selection
    }
    
    # Run tests for both tables
    table_configs = [table_config_lineitem, table_config_orders]
    
    # Connection parameters for ClickHouse
    connection_params = {
        'api_user': 'default',
        'api_user_password': None,  # Will be read from file
        'password_file': '~/.snowsql/clickhouse'
    }
    
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
        print(f"   Target Table: {table_config['database']}.{table_config['table']}")
        print(f"   Key Column: {table_config['key_column']} (range: {table_config['key_range'][0]}-{table_config['key_range'][1]})")
        print(f"   Select Column: {table_config['select_column']}")
        
        # Run the load test suite using shared framework
        test_results = run_load_test_suite(
            connection_params=connection_params,
            create_connection_func=create_clickhouse_connection,
            execute_query_func=execute_clickhouse_query,
            test_connection_func=test_clickhouse_connection,
            qps_values=qps_values,
            table_config=table_config,
            warmup_seconds=warmup_seconds,
            run_seconds=run_seconds,
            database_name="ClickHouse"
        )
        
        if test_results:
            # Generate reports using shared framework
            csv_file, graph_file = generate_reports(test_results, table_config, "ClickHouse", "ClickHouse (4 core, 16GB)")
            
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
    print(f"\n🎉 All ClickHouse load tests completed!")
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