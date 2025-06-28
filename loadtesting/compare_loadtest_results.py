#!/usr/bin/env python3
"""
Load Test Results Comparison Script

This script analyzes CSV files from Snowflake and ClickHouse load tests
and generates comparative performance graphs showing both systems side-by-side.
"""

import sys
from shared_loadtest import (
    find_loadtest_csv_files,
    load_csv_data,
    generate_comparison_graph
)


def print_performance_summary(snowflake_data, clickhouse_data, table_name):
    """
    Print a summary of performance comparison
    
    Args:
        snowflake_data: DataFrame with Snowflake results
        clickhouse_data: DataFrame with ClickHouse results
        table_name: Name of the table being compared
    """
    print(f"\n📊 Performance Summary for {table_name.upper()} Table:")
    print("=" * 60)
    
    if snowflake_data is not None and clickhouse_data is not None:
        # Find common QPS values for comparison
        common_qps = set(snowflake_data['qps']).intersection(set(clickhouse_data['qps']))
        
        if common_qps:
            print(f"{'QPS':<6} {'Snowflake Median':<18} {'ClickHouse Median':<18} {'Winner':<12}")
            print("-" * 60)
            
            for qps in sorted(common_qps):
                sf_row = snowflake_data[snowflake_data['qps'] == qps].iloc[0]
                ch_row = clickhouse_data[clickhouse_data['qps'] == qps].iloc[0]
                
                sf_median = sf_row['median']
                ch_median = ch_row['median']
                
                winner = "ClickHouse" if ch_median < sf_median else "Snowflake"
                improvement = abs(sf_median - ch_median) / max(sf_median, ch_median) * 100
                
                print(f"{qps:<6} {sf_median:<18.4f} {ch_median:<18.4f} {winner:<12} ({improvement:.1f}% better)")
            
            print("\n" + "=" * 60)
            print(f"{'QPS':<6} {'Snowflake P99':<18} {'ClickHouse P99':<18} {'Winner':<12}")
            print("-" * 60)
            
            for qps in sorted(common_qps):
                sf_row = snowflake_data[snowflake_data['qps'] == qps].iloc[0]
                ch_row = clickhouse_data[clickhouse_data['qps'] == qps].iloc[0]
                
                sf_p99 = sf_row['p99']
                ch_p99 = ch_row['p99']
                
                winner = "ClickHouse" if ch_p99 < sf_p99 else "Snowflake"
                improvement = abs(sf_p99 - ch_p99) / max(sf_p99, ch_p99) * 100
                
                print(f"{qps:<6} {sf_p99:<18.4f} {ch_p99:<18.4f} {winner:<12} ({improvement:.1f}% better)")
    
    elif snowflake_data is not None:
        print("Only Snowflake data available")
        print(snowflake_data[['qps', 'median', 'p99']])
    
    elif clickhouse_data is not None:
        print("Only ClickHouse data available") 
        print(clickhouse_data[['qps', 'median', 'p99']])
    
    else:
        print("No data available for comparison")


def main():
    """
    Main function to generate load test comparison graphs
    """
    print("🚀 Load Test Results Comparison Tool")
    print("=" * 50)
    
    # Find all CSV files
    csv_files = find_loadtest_csv_files()
    
    print("📁 Found CSV files:")
    for db, tables in csv_files.items():
        print(f"   {db.title()}:")
        for table, filepath in tables.items():
            print(f"     - {table}: {filepath}")
    
    if not any(csv_files.values()):
        print("❌ No load test CSV files found!")
        print("Expected files like: snowflake_loadtest_results_*.csv, clickhouse_loadtest_results_*.csv")
        sys.exit(1)
    
    # Process each table that has data
    all_tables = set()
    for tables in csv_files.values():
        all_tables.update(tables.keys())
    
    generated_graphs = []
    
    for table in sorted(all_tables):
        print(f"\n🔍 Processing {table.upper()} table...")
        
        # Load data for both databases
        snowflake_data = None
        clickhouse_data = None
        
        if table in csv_files['snowflake']:
            print(f"   Loading Snowflake data from {csv_files['snowflake'][table]}")
            snowflake_data = load_csv_data(csv_files['snowflake'][table])
        
        if table in csv_files['clickhouse']:
            print(f"   Loading ClickHouse data from {csv_files['clickhouse'][table]}")
            clickhouse_data = load_csv_data(csv_files['clickhouse'][table])
        
        if snowflake_data is None and clickhouse_data is None:
            print(f"   ❌ No valid data found for {table} table")
            continue
        
        # Generate comparison graph
        graph_file = generate_comparison_graph(
            csv_files['snowflake'].get(table), 
            csv_files['clickhouse'].get(table), 
            table
        )
        generated_graphs.append(graph_file)
        
        # Print performance summary
        print_performance_summary(snowflake_data, clickhouse_data, table)
    
    # Final summary
    print(f"\n🎉 Comparison complete!")
    if generated_graphs:
        print(f"📈 Generated comparison graphs:")
        for graph in generated_graphs:
            print(f"   - {graph}")
    else:
        print("❌ No graphs could be generated")


if __name__ == "__main__":
    main() 