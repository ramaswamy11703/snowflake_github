#!/usr/bin/env python3
"""
Shared Load Testing Framework

This module provides common functionality for running database load tests
against different database systems (Snowflake, ClickHouse, etc.).
"""

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


def execute_single_query(connection_func, query_func, results_queue, table_config, is_warmup=False):
    """
    Execute a single query using database-specific connection and query functions
    
    Args:
        connection_func: Function that returns (conn, cursor) or equivalent
        query_func: Function that executes query given conn, cursor, table_config, random_key
        results_queue: Queue to store results
        table_config: Dictionary containing table configuration
        is_warmup: Whether this is a warmup query (results ignored)
    """
    try:
        # Generate random key within the specified range
        random_key = random.randint(table_config['key_range'][0], table_config['key_range'][1])
        
        start_time = time.time()
        result_count = query_func(table_config, random_key)
        end_time = time.time()
        
        query_time = end_time - start_time
        
        # Only record results if not warmup
        if not is_warmup:
            results_queue.put({
                'success': True,
                'time': query_time,
                'key': random_key,
                'result_count': result_count
            })
        
    except Exception as e:
        if not is_warmup:
            results_queue.put({
                'success': False,
                'error': str(e),
                'time': 0
            })


def query_worker(worker_id, connection_params, create_connection_func, execute_query_func, query_queue, results_queue):
    """
    Worker thread that maintains one connection and processes multiple queries
    
    Args:
        worker_id: Unique identifier for this worker
        connection_params: Connection parameters (database-specific)
        create_connection_func: Function to create database connection
        execute_query_func: Function to execute queries
        query_queue: Queue to get query tasks from
        results_queue: Queue to put results in
    """
    conn = None
    
    try:
        # Create one connection for this worker using database-specific function
        conn = create_connection_func(connection_params)
        
        # Process queries from the queue
        while True:
            try:
                # Get a query task (blocking with timeout)
                task = query_queue.get(timeout=1.0)
                if task is None:  # Poison pill to terminate
                    break
                
                is_warmup = task.get('is_warmup', False)
                table_config = task.get('table_config')
                
                # Execute query using database-specific function
                try:
                    random_key = random.randint(table_config['key_range'][0], table_config['key_range'][1])
                    start_time = time.time()
                    result_count = execute_query_func(conn, table_config, random_key)
                    end_time = time.time()
                    
                    query_time = end_time - start_time
                    
                    # Only record results if not warmup
                    if not is_warmup:
                        results_queue.put({
                            'success': True,
                            'time': query_time,  
                            'key': random_key,
                            'result_count': result_count
                        })
                    
                except Exception as e:
                    if not is_warmup:
                        results_queue.put({
                            'success': False,
                            'error': str(e),
                            'time': 0
                        })
                
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
        # Clean up connection using database-specific cleanup
        if conn:
            try:
                if hasattr(conn, 'close'):
                    conn.close()
                elif hasattr(conn, 'disconnect'):
                    conn.disconnect()
            except:
                pass


def run_qps_load_test(connection_params, create_connection_func, execute_query_func, 
                      target_qps, table_config, warmup_seconds=30, run_seconds=60):
    """
    Run a QPS-based load test with warmup period using persistent connections
    
    Args:
        connection_params: Database-specific connection parameters
        create_connection_func: Function to create database connections
        execute_query_func: Function to execute queries
        target_qps: Target queries per second
        table_config: Dictionary containing table configuration
        warmup_seconds: Duration of warmup phase
        run_seconds: Duration of actual test phase
    """
    print(f"\n=== Starting QPS Load Test ===")
    print(f"Target Table: {table_config['database']}.{table_config.get('schema', '')}.{table_config['table']}")
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
            args=(i, connection_params, create_connection_func, execute_query_func, query_queue, results_queue),
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


def run_load_test_suite(connection_params, create_connection_func, execute_query_func, test_connection_func,
                       qps_values, table_config, warmup_seconds=60, run_seconds=120, database_name="Database"):
    """
    Run a comprehensive load test suite across multiple QPS values
    
    Args:
        connection_params: Database-specific connection parameters
        create_connection_func: Function to create database connections
        execute_query_func: Function to execute queries
        test_connection_func: Function to test basic connectivity
        qps_values: List of QPS values to test
        table_config: Dictionary containing table configuration
        warmup_seconds: Duration of warmup phase
        run_seconds: Duration of actual test phase
        database_name: Name of database system for reporting
    """
    print(f"\n🚀 Starting comprehensive {database_name} load test suite")
    print(f"   Target Table: {table_config['database']}.{table_config.get('schema', '')}.{table_config['table']}")
    print(f"   Key Column: {table_config['key_column']} (range: {table_config['key_range'][0]}-{table_config['key_range'][1]})")
    print(f"   Select Column: {table_config['select_column']}")
    print(f"   QPS values to test: {qps_values}")
    print(f"   Warmup duration: {warmup_seconds}s")
    print(f"   Test duration: {run_seconds}s")
    print(f"   Total estimated time: {len(qps_values) * (warmup_seconds + run_seconds + 10) / 60:.1f} minutes")
    
    # Test basic connectivity first
    if not test_connection_func():
        print(f"❌ Failed to establish basic {database_name} connection")
        return None
    
    print(f"\n🎉 {database_name} connection is working properly!")
    
    # Store results for CSV and graphing
    test_results = []
    
    # Run load tests for each QPS value
    for i, target_qps in enumerate(qps_values):
        print(f"\n{'='*60}")
        print(f"🧪 Running test {i+1}/{len(qps_values)}: {target_qps} QPS")
        print(f"{'='*60}")
        
        results = run_qps_load_test(connection_params, create_connection_func, execute_query_func,
                                  target_qps, table_config, warmup_seconds, run_seconds)
        
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
    
    return test_results


def generate_reports(test_results, table_config, database_name="Database"):
    """
    Generate CSV reports and performance graphs from test results
    
    Args:
        test_results: List of test result dictionaries
        table_config: Dictionary containing table configuration
        database_name: Name of database system for file naming
    """
    # Generate CSV
    csv_filename = f"{database_name.lower()}_loadtest_results_{table_config['table']}.csv"
    
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
        plt.title(f'{database_name} Load Test Results: Response Time vs QPS for table {table_config["table"]}', fontsize=14)
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
        graph_filename = f"{database_name.lower()}_loadtest_graph_{table_config['table']}.png"
        plt.savefig(graph_filename, dpi=300, bbox_inches='tight')
        print(f"✅ Graph saved as: {graph_filename}")
        plt.show()
        
        return csv_filename, graph_filename
    else:
        print("❌ No successful test results to graph")
        return csv_filename, None 