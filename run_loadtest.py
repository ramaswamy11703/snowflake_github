#!/usr/bin/env python3
"""
Load Test Runner Convenience Script

This script provides easy access to the load testing functionality
from the top level directory without needing to navigate to loadtesting/
"""

import sys
import os
import subprocess
import argparse

# Add loadtesting directory to path
loadtest_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'loadtesting')
sys.path.insert(0, loadtest_dir)

def run_script(script_name, args=None):
    """
    Run a script from the loadtesting directory
    
    Args:
        script_name: Name of the script to run
        args: Additional arguments to pass to the script
    """
    script_path = os.path.join(loadtest_dir, script_name)
    
    if not os.path.exists(script_path):
        print(f"❌ Script not found: {script_path}")
        return False
    
    cmd = [sys.executable, script_path]
    if args:
        cmd.extend(args)
    
    print(f"🚀 Running: {' '.join(cmd)}")
    print("=" * 50)
    
    try:
        result = subprocess.run(cmd, cwd=loadtest_dir)
        return result.returncode == 0
    except Exception as e:
        print(f"❌ Error running script: {e}")
        return False

def main():
    """
    Main function to handle command line arguments
    """
    parser = argparse.ArgumentParser(
        description="Convenience script to run load tests",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s snowflake              # Run Snowflake load tests
  %(prog)s clickhouse             # Run ClickHouse load tests
  %(prog)s graphs --all           # Generate all graphs
  %(prog)s graphs --info          # Show CSV file info
  %(prog)s compare                # Compare results between databases
        """
    )
    
    parser.add_argument('command', choices=['snowflake', 'clickhouse', 'graphs', 'compare'],
                       help='Load test command to run')
    parser.add_argument('args', nargs=argparse.REMAINDER,
                       help='Additional arguments to pass to the script')
    
    args = parser.parse_args()
    
    script_map = {
        'snowflake': 'snowflake_loadtest.py',
        'clickhouse': 'clickhouse_loadtest.py', 
        'graphs': 'generate_graphs_from_csv.py',
        'compare': 'compare_loadtest_results.py'
    }
    
    script_name = script_map[args.command]
    success = run_script(script_name, args.args)
    
    if not success:
        sys.exit(1)

if __name__ == "__main__":
    main() 