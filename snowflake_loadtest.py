#!/usr/bin/env python3
"""
Snowflake Database Connection Script

This script connects to a Snowflake database using configuration
from the SnowSQL config file located at ~/.snowsql/config
"""

import configparser
import os
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.backends import default_backend
import sys
import random
import time
import statistics
import threading
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
import datetime
import csv
import matplotlib.pyplot as plt


def load_private_key(private_key_path):
    """
    Load the private key from the specified path for JWT authentication
    """
    try:
        with open(private_key_path, 'rb') as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None,  # Assuming no password on the private key
                backend=default_backend()
            )
        
        # Convert to DER format for Snowflake connector
        private_key_der = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        
        return private_key_der
    except Exception as e:
        print(f"Error loading private key: {e}")
        return None


def read_snowsql_config(config_path="~/.snowsql/config"):
    """
    Read the SnowSQL configuration file and return connection parameters
    """
    config_path = os.path.expanduser(config_path)
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at {config_path}")
    
    config = configparser.ConfigParser()
    config.read(config_path)
    
    return config


def connect_to_snowflake(connection_name="my_conn", config_path="~/.snowsql/config"):
    """
    Connect to Snowflake using parameters from SnowSQL config file
    
    Args:
        connection_name (str): Name of the connection in the config file
        config_path (str): Path to the SnowSQL config file
    
    Returns:
        snowflake.connector.connection.SnowflakeConnection: Database connection object
    """
    try:
        # Read configuration
        config = read_snowsql_config(config_path)
        
        # Try to get connection parameters from the specified connection
        section_name = f"connections.{connection_name}"
        if section_name not in config:
            # Fall back to account name section if available
            available_sections = [s for s in config.sections() if not s.startswith('connections.')]
            if available_sections:
                section_name = available_sections[0]
                print(f"Connection '{connection_name}' not found, using '{section_name}'")
            else:
                raise ValueError(f"No valid connection configuration found")
        
        section = config[section_name]
        
        # Extract connection parameters
        account = section.get('accountname', section.get('account'))
        username = section.get('username', section.get('user'))
        private_key_path = section.get('private_key_path')
        
        # Remove quotes from paths if present
        if private_key_path:
            private_key_path = private_key_path.strip('"\'')
        if account:
            account = account.strip('"\'')
        if username:
            username = username.strip('"\'')
        
        if not all([account, username, private_key_path]):
            raise ValueError("Missing required connection parameters (account, username, private_key_path)")
        
        # Load private key
        private_key_der = load_private_key(os.path.expanduser(private_key_path))
        if private_key_der is None:
            raise ValueError("Failed to load private key")
        
        print(f"Connecting to Snowflake...")
        print(f"Account: {account}")
        print(f"Username: {username}")
        print(f"Using JWT authentication with private key")
        
        # Create connection
        conn = snowflake.connector.connect(
            account=account,
            user=username,
            private_key=private_key_der,
            authenticator='SNOWFLAKE_JWT'
        )
        
        print("✅ Successfully connected to Snowflake!")
        return conn
        
    except Exception as e:
        print(f"❌ Error connecting to Snowflake: {e}")
        return None


def test_connection(conn):
    """
    Test the connection by running a simple query
    """
    if conn is None:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION(), CURRENT_USER(), CURRENT_ACCOUNT()")
        result = cursor.fetchone()
        
        print("\n=== Connection Test Results ===")
        print(f"Snowflake Version: {result[0]}")
        print(f"Current User: {result[1]}")
        print(f"Current Account: {result[2]}")
        
        cursor.close()
        return True
        
    except Exception as e:
        print(f"❌ Error testing connection: {e}")
        return False


def execute_single_query(conn, cursor, results_queue, table_config, is_warmup=False):
    """
    Execute a single query using existing connection and cursor
    
    Args:
        conn: Existing Snowflake connection
        cursor: Existing cursor for the connection
        results_queue: Queue to store results
        table_config: Dictionary containing table configuration (database, schema, table, key_column, select_column, key_range)
        is_warmup: Whether this is a warmup query (results ignored)
    """
    try:
        # Generate random key within the specified range
        random_key = random.randint(table_config['key_range'][0], table_config['key_range'][1])
        
        query = f"""
        SELECT {table_config['select_column']} 
        FROM {table_config['database']}.{table_config['schema']}.{table_config['table']} 
        WHERE {table_config['key_column']} = {random_key}
        """
        
        start_time = time.time()
        cursor.execute(query)
        results = cursor.fetchall()
        end_time = time.time()
        
        query_time = end_time - start_time
        
        # Only record results if not warmup
        if not is_warmup:
            results_queue.put({
                'success': True,
                'time': query_time,
                'key': random_key,
                'result_count': len(results)
            })
        
    except Exception as e:
        if not is_warmup:
            results_queue.put({
                'success': False,
                'error': str(e),
                'time': 0
            })


def query_worker(worker_id, conn_params, query_queue, results_queue):
    """
    Worker thread that maintains one connection and processes multiple queries
    
    Args:
        worker_id: Unique identifier for this worker
        conn_params: Connection parameters dict
        query_queue: Queue to get query tasks from
        results_queue: Queue to put results in
    """
    conn = None
    cursor = None
    
    try:
        # Create one connection for this worker
        conn = snowflake.connector.connect(
            account=conn_params['account'],
            user=conn_params['user'],
            private_key=conn_params['private_key'],
            authenticator='SNOWFLAKE_JWT'
        )
        
        cursor = conn.cursor()
        cursor.execute("USE WAREHOUSE loadtest")
        
        # Process queries from the queue
        while True:
            try:
                # Get a query task (blocking with timeout)
                task = query_queue.get(timeout=1.0)
                if task is None:  # Poison pill to terminate
                    break
                
                is_warmup = task.get('is_warmup', False)
                table_config = task.get('table_config')
                execute_single_query(conn, cursor, results_queue, table_config, is_warmup)
                query_queue.task_done()
                
            except Exception as queue_error:
                # Queue timeout or other queue error, check if we should continue
                if query_queue.empty():
                    time.sleep(0.1)
                    continue
                else:
                    break
                    
    except Exception as e:
        print(f"❌ Worker {worker_id} connection error: {e}")
    finally:
        # Clean up connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def run_qps_load_test(conn_params, target_qps, table_config, warmup_seconds=30, run_seconds=60):
    """
    Run a QPS-based load test with warmup period using persistent connections
    
    Args:
        conn_params: Connection parameters dict
        target_qps: Target queries per second
        table_config: Dictionary containing table configuration (database, schema, table, key_column, select_column, key_range)
        warmup_seconds: Duration of warmup phase
        run_seconds: Duration of actual test phase
    """
    print(f"\n=== Starting QPS Load Test ===")
    print(f"Target Table: {table_config['database']}.{table_config['schema']}.{table_config['table']}")
    print(f"Target QPS: {target_qps}")
    print(f"Warmup period: {warmup_seconds} seconds")
    print(f"Test duration: {run_seconds} seconds")
    print(f"Worker threads: {target_qps}")
    
    query_queue = Queue()
    results_queue = Queue()
    
    # Start worker threads
    workers = []
    print(f"🔧 Starting {target_qps} worker threads...")
    for i in range(target_qps):
        worker = threading.Thread(
            target=query_worker,
            args=(i, conn_params, query_queue, results_queue),
            daemon=True
        )
        worker.start()
        workers.append(worker)
    
    # Give workers time to establish connections
    time.sleep(2)
    print("✅ Worker threads started")
    
    # Calculate intervals between query starts
    interval = 1.0 / target_qps
    
    # Phase 1: Warmup
    print(f"\n🔥 Starting warmup phase ({warmup_seconds}s)...")
    warmup_start = time.time()
    warmup_end = warmup_start + warmup_seconds
    next_query_time = warmup_start
    
    while time.time() < warmup_end:
        current_time = time.time()
        if current_time >= next_query_time:
            query_queue.put({'is_warmup': True, 'table_config': table_config})
            next_query_time += interval
        else:
            time.sleep(0.001)  # Small sleep to prevent busy waiting
    
    print("✅ Warmup phase completed")
    
    # Phase 2: Actual test
    print(f"\n⚡ Starting load test phase ({run_seconds}s)...")
    test_start = time.time()
    queries_submitted = 0
    test_end = test_start + run_seconds
    next_query_time = test_start
    
    while time.time() < test_end:
        current_time = time.time()
        if current_time >= next_query_time:
            query_queue.put({'is_warmup': False, 'table_config': table_config})
            queries_submitted += 1
            next_query_time += interval
            
            if queries_submitted % (target_qps * 10) == 0:  # Progress every 10 seconds
                elapsed = current_time - test_start
                print(f"  Elapsed: {elapsed:.1f}s, Queries submitted: {queries_submitted}")
        else:
            time.sleep(0.001)  # Small sleep to prevent busy waiting
    
    print("✅ Load test phase completed")
    print(f"📊 Collecting results...")
    
    # Wait for remaining queries to complete
    query_queue.join()
    
    # Send poison pills to terminate workers
    for _ in range(target_qps):
        query_queue.put(None)
    
    # Wait for workers to finish
    for worker in workers:
        worker.join(timeout=5.0)
    
    # Collect results
    query_times = []
    successful_queries = 0
    failed_queries = 0
    
    while not results_queue.empty():
        result = results_queue.get()
        if result['success']:
            query_times.append(result['time'])
            successful_queries += 1
        else:
            failed_queries += 1
    
    # Calculate statistics
    if query_times:
        # Sort query times for percentile calculations
        sorted_times = sorted(query_times)
        
        # Basic statistics
        avg_time = sum(query_times) / len(query_times)
        min_time = min(query_times)
        max_time = max(query_times)
        median_time = statistics.median(sorted_times)
        
        # Calculate percentiles
        def percentile(data, p):
            """Calculate the p-th percentile of data"""
            n = len(data)
            if n == 0:
                return 0
            index = (p / 100) * (n - 1)
            if index.is_integer():
                return data[int(index)]
            else:
                lower = data[int(index)]
                upper = data[int(index) + 1]
                return lower + (upper - lower) * (index - int(index))
        
        p90 = percentile(sorted_times, 90)
        p95 = percentile(sorted_times, 95)
        p99 = percentile(sorted_times, 99)
        
        # Calculate actual QPS achieved
        actual_qps = successful_queries / run_seconds
        
        print(f"\n=== Load Test Results ===")
        print(f"Target QPS: {target_qps}")
        print(f"Actual QPS: {actual_qps:.2f}")
        print(f"Queries submitted: {queries_submitted}")
        print(f"Successful queries: {successful_queries}")
        print(f"Failed queries: {failed_queries}")
        print(f"Success rate: {(successful_queries / queries_submitted * 100):.1f}%")
        
        # Return performance metrics as tuple
        return (min_time, max_time, avg_time, median_time, p90, p95, p99, actual_qps, successful_queries, failed_queries)
    else:
        print("❌ No successful queries completed")
        return (0, 0, 0, 0, 0, 0, 0, 0, 0, failed_queries)


def get_connection_params(config_path):
    """
    Extract connection parameters for use in threading
    """
    try:
        config = read_snowsql_config(config_path)
        
        # Try to get connection parameters from the specified connection
        section_name = "connections.my_conn"
        if section_name not in config:
            # Fall back to account name section if available
            available_sections = [s for s in config.sections() if not s.startswith('connections.')]
            if available_sections:
                section_name = available_sections[0]
        
        section = config[section_name]
        
        # Extract connection parameters
        account = section.get('accountname', section.get('account'))
        username = section.get('username', section.get('user'))
        private_key_path = section.get('private_key_path')
        
        # Remove quotes from paths if present
        if private_key_path:
            private_key_path = private_key_path.strip('"\'')
        if account:
            account = account.strip('"\'')
        if username:
            username = username.strip('"\'')  
        
        # Load private key
        private_key_der = load_private_key(os.path.expanduser(private_key_path))
        if private_key_der is None:
            raise ValueError("Failed to load private key")
        
        return {
            'account': account,
            'user': username,
            'private_key': private_key_der
        }
        
    except Exception as e:
        print(f"❌ Error getting connection parameters: {e}")
        return None


def main():
    """
    Main function to run multiple QPS-based load tests and generate CSV + graph
    """
    config_path = "/Users/srramaswamy/.snowsql/config"
    
    # Test basic connection first
    conn = connect_to_snowflake("my_conn", config_path)
    if conn:
        if not test_connection(conn):
            print("Failed to establish connection.")
            sys.exit(1)
        print("\n🎉 Connection is working properly!")
        conn.close()
    else:
        print("Failed to establish connection.")
        sys.exit(1)
    
    # Get connection parameters for threading
    conn_params = get_connection_params(config_path)
    if not conn_params:
        print("Failed to get connection parameters.")
        sys.exit(1)
    
    # Configure test parameters
    qps_values = [10, 25, 50, 100, 200, 300]
    warmup_seconds = 60    # Warmup duration 
    run_seconds = 120      # Test duration
    
    # Configure table to test against - MODIFY THIS TO CHANGE TARGET TABLE
    # 
    # Examples:
    # For ORDERS table:
    #   table: 'ORDERS', key_column: 'O_ORDERKEY', select_column: 'O_COMMENT', key_range: [1, 6000000] 
    # For LINEITEM table:
    #   table: 'LINEITEM', key_column: 'L_ORDERKEY', select_column: 'L_COMMENT', key_range: [1, 6000000]
    # For CUSTOMER table: 
    #   table: 'CUSTOMER', key_column: 'C_CUSTKEY', select_column: 'C_NAME', key_range: [1, 150000]
    # For PART table:
    #   table: 'PART', key_column: 'P_PARTKEY', select_column: 'P_NAME', key_range: [1, 200000]
    #
    table_config = {
        'database': 'SNOWFLAKE_SAMPLE_DATA',
        'schema': 'TPCH_SF1', 
        'table': 'ORDERS',
        'key_column': 'O_ORDERKEY',
        'select_column': 'O_COMMENT',
        'key_range': [1, 6000000]  # Range for random O_ORDERKEY selection
    }
    
    table_config_old = {
        'database': 'SNOWFLAKE_SAMPLE_DATA',
        'schema': 'TPCH_SF1', 
        'table': 'LINEITEM',
        'key_column': 'L_ORDERKEY',
        'select_column': 'L_COMMENT',
        'key_range': [1, 6000000]  # Range for random O_ORDERKEY selection
    }
    print(f"\n🚀 Starting comprehensive load test suite")
    print(f"   Target Table: {table_config['database']}.{table_config['schema']}.{table_config['table']}")
    print(f"   Key Column: {table_config['key_column']} (range: {table_config['key_range'][0]}-{table_config['key_range'][1]})")
    print(f"   Select Column: {table_config['select_column']}")
    print(f"   QPS values to test: {qps_values}")
    print(f"   Warmup duration: {warmup_seconds}s")
    print(f"   Test duration: {run_seconds}s")
    print(f"   Total estimated time: {len(qps_values) * (warmup_seconds + run_seconds + 10) / 60:.1f} minutes")
    
    # Store results for CSV and graphing
    test_results = []
    
    # Run load tests for each QPS value
    for i, target_qps in enumerate(qps_values):
        print(f"\n{'='*60}")
        print(f"🧪 Running test {i+1}/{len(qps_values)}: {target_qps} QPS")
        print(f"{'='*60}")
        
        results = run_qps_load_test(conn_params, target_qps, table_config, warmup_seconds, run_seconds)
        
        if results:
            min_time, max_time, avg_time, median_time, p90, p95, p99, actual_qps, successful_queries, failed_queries = results
            
            # Store results
            test_results.append({
                'target_qps': target_qps,
                'actual_qps': actual_qps,
                'min': min_time,
                'avg': avg_time,
                'median': median_time,
                'p90': p90,
                'p95': p95,
                'p99': p99,
                'max': max_time,
                'successful': successful_queries,
                'failed': failed_queries
            })
            
            print(f"\n✅ Test {i+1} completed:")
            print(f"   Target QPS: {target_qps}, Actual: {actual_qps:.2f}")
            print(f"   Median: {median_time:.4f}s, P99: {p99:.4f}s")
            print(f"   Success rate: {(successful_queries / (successful_queries + failed_queries) * 100):.1f}%")
        else:
            print(f"❌ Test {i+1} failed")
            # Add placeholder data for failed test
            test_results.append({
                'target_qps': target_qps,
                'actual_qps': 0,
                'min': 0,
                'avg': 0,
                'median': 0,
                'p90': 0,
                'p95': 0,
                'p99': 0,
                'max': 0,
                'successful': 0,
                'failed': 0
            })
    
    # Generate CSV
    csv_filename = f"snowflake_loadtest_results_{table_config['table']}.csv"
    
    print(f"\n📊 Generating CSV report: {csv_filename}")
    
    with open(csv_filename, 'w', newline='') as csvfile:
        fieldnames = ['qps', 'min', 'avg', 'median', 'p90', 'p95', 'p99', 'max', 'actual_qps', 'successful', 'failed']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for result in test_results:
            writer.writerow({
                'qps': result['target_qps'],
                'min': f"{result['min']:.4f}",
                'avg': f"{result['avg']:.4f}",
                'median': f"{result['median']:.4f}",
                'p90': f"{result['p90']:.4f}",
                'p95': f"{result['p95']:.4f}",
                'p99': f"{result['p99']:.4f}",
                'max': f"{result['max']:.4f}",
                'actual_qps': f"{result['actual_qps']:.2f}",
                'successful': result['successful'],
                'failed': result['failed']
            })
    
    # Print CSV contents
    print(f"\n📋 CSV Contents:")
    print("-" * 120)
    with open(csv_filename, 'r') as csvfile:
        print(csvfile.read())
    
    # Create graph
    print(f"📈 Generating performance graph...")
    
    # Extract data for graphing (only successful tests)
    successful_results = [r for r in test_results if r['successful'] > 0]
    
    if successful_results:
        qps_vals = [r['target_qps'] for r in successful_results]
        median_vals = [r['median'] for r in successful_results]
        p99_vals = [r['p99'] for r in successful_results]
        
        plt.figure(figsize=(12, 8))
        plt.plot(qps_vals, median_vals, 'o-', label='Median Response Time', linewidth=2, markersize=8)
        plt.plot(qps_vals, p99_vals, 's-', label='99th Percentile Response Time', linewidth=2, markersize=8)
        
        plt.xlabel('Target QPS', fontsize=12)
        plt.ylabel('Response Time (seconds)', fontsize=12)
        plt.title('Snowflake Load Test Results: Response Time vs QPS', fontsize=14)
        plt.legend(fontsize=11)
        plt.grid(True, alpha=0.3)
        plt.yscale('log')  # Log scale for better visualization
        
        # Annotate points with values
        for i, (qps, median, p99) in enumerate(zip(qps_vals, median_vals, p99_vals)):
            plt.annotate(f'{median:.3f}', (qps, median), textcoords="offset points", 
                        xytext=(0,10), ha='center', fontsize=9)
            plt.annotate(f'{p99:.3f}', (qps, p99), textcoords="offset points", 
                        xytext=(0,10), ha='center', fontsize=9)
        
        plt.tight_layout()
        graph_filename = f"snowflake_loadtest_graph_{table_config['table']}.png"
        plt.savefig(graph_filename, dpi=300, bbox_inches='tight')
        print(f"✅ Graph saved as: {graph_filename}")
        plt.show()
    else:
        print("❌ No successful test results to graph")
    
    print(f"\n🎉 Load test suite completed!")
    print(f"   CSV report: {csv_filename}")
    if successful_results:
        print(f"   Performance graph: {graph_filename}")
 


if __name__ == "__main__":
    main() 