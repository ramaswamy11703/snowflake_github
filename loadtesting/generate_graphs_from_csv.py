#!/usr/bin/env python3
"""
Generate Performance Graphs from CSV Files

This script regenerates performance graphs from existing load test CSV files.
It can generate both individual database graphs and comparison graphs.
"""

import sys
import os
import argparse
from shared_loadtest import (
    find_loadtest_csv_files, 
    generate_single_database_graph, 
    generate_comparison_graph,
    load_csv_data
)


def generate_individual_graphs():
    """
    Generate individual performance graphs for each found CSV file
    """
    print("🔍 Finding load test CSV files...")
    csv_files = find_loadtest_csv_files()
    
    if not any(csv_files.values()):
        print("❌ No load test CSV files found!")
        print("Expected files like: snowflake_loadtest_results_*.csv, clickhouse_loadtest_results_*.csv")
        return []
    
    generated_graphs = []
    
    # Generate Snowflake graphs
    for table, csv_file in csv_files['snowflake'].items():
        print(f"📊 Generating Snowflake graph for {table.upper()} table...")
        graph_file = generate_single_database_graph(
            csv_file, 
            "Snowflake", 
            "Snowflake XS (8 core, 16GB)"
        )
        if graph_file:
            generated_graphs.append(graph_file)
            print(f"   ✅ Generated: {graph_file}")
        else:
            print(f"   ❌ Failed to generate graph for {table}")
    
    # Generate ClickHouse graphs
    for table, csv_file in csv_files['clickhouse'].items():
        print(f"📊 Generating ClickHouse graph for {table.upper()} table...")
        graph_file = generate_single_database_graph(
            csv_file, 
            "ClickHouse", 
            "ClickHouse (4 core, 16GB)"
        )
        if graph_file:
            generated_graphs.append(graph_file)
            print(f"   ✅ Generated: {graph_file}")
        else:
            print(f"   ❌ Failed to generate graph for {table}")
    
    return generated_graphs


def generate_comparison_graphs():
    """
    Generate comparison graphs between Snowflake and ClickHouse
    """
    print("🔍 Finding load test CSV files for comparison...")
    csv_files = find_loadtest_csv_files()
    
    # Find tables that have data from both databases
    all_tables = set()
    for tables in csv_files.values():
        all_tables.update(tables.keys())
    
    generated_graphs = []
    
    for table in sorted(all_tables):
        print(f"📊 Generating comparison graph for {table.upper()} table...")
        
        snowflake_csv = csv_files['snowflake'].get(table)
        clickhouse_csv = csv_files['clickhouse'].get(table)
        
        if snowflake_csv or clickhouse_csv:
            graph_file = generate_comparison_graph(snowflake_csv, clickhouse_csv, table)
            if graph_file:
                generated_graphs.append(graph_file)
                print(f"   ✅ Generated: {graph_file}")
            else:
                print(f"   ❌ Failed to generate comparison graph for {table}")
        else:
            print(f"   ⚠️  No data found for {table}")
    
    return generated_graphs


def show_csv_info():
    """
    Show information about available CSV files
    """
    print("📁 Available Load Test CSV Files:")
    print("=" * 50)
    
    csv_files = find_loadtest_csv_files()
    
    for db, tables in csv_files.items():
        print(f"\n{db.title()}:")
        if tables:
            for table, filepath in tables.items():
                # Load data to show basic info
                data = load_csv_data(filepath)
                if data is not None:
                    qps_range = f"{data['qps'].min()}-{data['qps'].max()}"
                    num_tests = len(data)
                    print(f"   📊 {table.upper()}: {filepath}")
                    print(f"      - QPS Range: {qps_range}")
                    print(f"      - Number of tests: {num_tests}")
                else:
                    print(f"   ❌ {table.upper()}: {filepath} (failed to load)")
        else:
            print(f"   (No CSV files found)")


def main():
    """
    Main function to handle command line arguments and generate graphs
    """
    parser = argparse.ArgumentParser(
        description="Generate performance graphs from load test CSV files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --individual        # Generate individual database graphs
  %(prog)s --comparison        # Generate comparison graphs
  %(prog)s --all              # Generate both individual and comparison graphs
  %(prog)s --info             # Show information about available CSV files
        """
    )
    
    parser.add_argument('--individual', action='store_true',
                       help='Generate individual performance graphs for each database')
    parser.add_argument('--comparison', action='store_true',
                       help='Generate comparison graphs between databases')
    parser.add_argument('--all', action='store_true',
                       help='Generate both individual and comparison graphs')
    parser.add_argument('--info', action='store_true',
                       help='Show information about available CSV files')
    
    args = parser.parse_args()
    
    # If no arguments, show help
    if not any([args.individual, args.comparison, args.all, args.info]):
        parser.print_help()
        return
    
    print("🚀 Load Test Graph Generator")
    print("=" * 50)
    
    if args.info:
        show_csv_info()
        return
    
    all_generated = []
    
    if args.individual or args.all:
        print("\n📈 Generating Individual Database Graphs")
        print("-" * 40)
        individual_graphs = generate_individual_graphs()
        all_generated.extend(individual_graphs)
    
    if args.comparison or args.all:
        print("\n📊 Generating Comparison Graphs")
        print("-" * 40)
        comparison_graphs = generate_comparison_graphs()
        all_generated.extend(comparison_graphs)
    
    # Summary
    print(f"\n🎉 Graph generation complete!")
    if all_generated:
        print(f"📈 Generated {len(all_generated)} graphs:")
        for graph in all_generated:
            print(f"   - {graph}")
    else:
        print("❌ No graphs were generated")


if __name__ == "__main__":
    main() 