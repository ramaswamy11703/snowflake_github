#!/usr/bin/env python3
"""
Load Test Results Comparison Script

This script analyzes CSV files from Snowflake and ClickHouse load tests
and generates comparative performance graphs showing both systems side-by-side.
"""

import pandas as pd
import matplotlib.pyplot as plt
import os
import glob
import sys
from pathlib import Path


def find_csv_files():
    """
    Find all loadtest CSV files in the current directory
    
    Returns:
        Dictionary with found CSV files organized by database and table
    """
    csv_files = {
        'snowflake': {},
        'clickhouse': {}
    }
    
    # Find all CSV files matching load test patterns
    patterns = [
        'snowflake_loadtest_results_*.csv',
        'clickhouse_loadtest_results_*.csv'
    ]
    
    for pattern in patterns:
        files = glob.glob(pattern)
        for file in files:
            filename = os.path.basename(file)
            
            if filename.startswith('snowflake_loadtest_results_'):
                table_name = filename.replace('snowflake_loadtest_results_', '').replace('.csv', '')
                csv_files['snowflake'][table_name.lower()] = file
            elif filename.startswith('clickhouse_loadtest_results_'):
                table_name = filename.replace('clickhouse_loadtest_results_', '').replace('.csv', '')
                csv_files['clickhouse'][table_name.lower()] = file
    
    return csv_files


def load_csv_data(filepath):
    """
    Load and parse CSV data from load test results
    
    Args:
        filepath: Path to CSV file
    
    Returns:
        DataFrame with load test results
    """
    try:
        df = pd.read_csv(filepath)
        # Convert string numbers to float
        numeric_columns = ['min', 'avg', 'median', 'p90', 'p95', 'p99', 'max', 'actual_qps']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        return df
    except Exception as e:
        print(f"❌ Error loading {filepath}: {e}")
        return None


def create_comparison_graph(snowflake_data, clickhouse_data, table_name):
    """
    Create a comparison graph for a specific table showing both databases
    
    Args:
        snowflake_data: DataFrame with Snowflake results
        clickhouse_data: DataFrame with ClickHouse results  
        table_name: Name of the table being compared
    """
    plt.figure(figsize=(14, 10))
    
    # Create two subplots: one for median, one for p99
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 12))
    
    # Colors for each database
    snowflake_color = '#29B5E8'  # Snowflake blue
    clickhouse_color = '#FFCC02'  # ClickHouse yellow
    
    # Plot 1: Median Response Times
    if snowflake_data is not None:
        ax1.plot(snowflake_data['qps'], snowflake_data['median'], 
                'o-', label='Snowflake', color=snowflake_color, 
                linewidth=3, markersize=8, markerfacecolor='white', markeredgewidth=2)
    
    if clickhouse_data is not None:
        ax1.plot(clickhouse_data['qps'], clickhouse_data['median'], 
                's-', label='ClickHouse', color=clickhouse_color,
                linewidth=3, markersize=8, markerfacecolor='white', markeredgewidth=2)
    
    ax1.set_xlabel('Queries Per Second (QPS)', fontsize=12)
    ax1.set_ylabel('Median Response Time (seconds)', fontsize=12)
    ax1.set_title(f'{table_name.upper()} Table - Median Response Time Comparison', fontsize=14, fontweight='bold')
    ax1.legend(fontsize=12)
    ax1.grid(True, alpha=0.3)
    ax1.set_yscale('log')
    
    # Annotate points with values for median
    if snowflake_data is not None:
        for qps, median in zip(snowflake_data['qps'], snowflake_data['median']):
            ax1.annotate(f'{median:.3f}', (qps, median), textcoords="offset points", 
                        xytext=(0,15), ha='center', fontsize=9, color=snowflake_color, fontweight='bold')
    
    if clickhouse_data is not None:
        for qps, median in zip(clickhouse_data['qps'], clickhouse_data['median']):
            ax1.annotate(f'{median:.3f}', (qps, median), textcoords="offset points", 
                        xytext=(0,-20), ha='center', fontsize=9, color=clickhouse_color, fontweight='bold')
    
    # Plot 2: P99 Response Times
    if snowflake_data is not None:
        ax2.plot(snowflake_data['qps'], snowflake_data['p99'], 
                'o-', label='Snowflake', color=snowflake_color,
                linewidth=3, markersize=8, markerfacecolor='white', markeredgewidth=2)
    
    if clickhouse_data is not None:
        ax2.plot(clickhouse_data['qps'], clickhouse_data['p99'], 
                's-', label='ClickHouse', color=clickhouse_color,
                linewidth=3, markersize=8, markerfacecolor='white', markeredgewidth=2)
    
    ax2.set_xlabel('Queries Per Second (QPS)', fontsize=12)
    ax2.set_ylabel('99th Percentile Response Time (seconds)', fontsize=12)
    ax2.set_title(f'{table_name.upper()} Table - 99th Percentile Response Time Comparison', fontsize=14, fontweight='bold')
    ax2.legend(fontsize=12)
    ax2.grid(True, alpha=0.3)
    ax2.set_yscale('log')
    
    # Annotate points with values for p99
    if snowflake_data is not None:
        for qps, p99 in zip(snowflake_data['qps'], snowflake_data['p99']):
            ax2.annotate(f'{p99:.3f}', (qps, p99), textcoords="offset points", 
                        xytext=(0,15), ha='center', fontsize=9, color=snowflake_color, fontweight='bold')
    
    if clickhouse_data is not None:
        for qps, p99 in zip(clickhouse_data['qps'], clickhouse_data['p99']):
            ax2.annotate(f'{p99:.3f}', (qps, p99), textcoords="offset points", 
                        xytext=(0,-20), ha='center', fontsize=9, color=clickhouse_color, fontweight='bold')
    
    plt.tight_layout()
    
    # Save the graph
    filename = f"loadtest_comparison_{table_name.lower()}.png"
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    print(f"✅ Comparison graph saved: {filename}")
    
    return filename


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
    csv_files = find_csv_files()
    
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
        graph_file = create_comparison_graph(snowflake_data, clickhouse_data, table)
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