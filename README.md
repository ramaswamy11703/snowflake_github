# snowflake_github

Load test scripts to compare clickhouse / snowflake performance.

This is on a 1% sample of tpc-h.

The lineitem table is roughly 160meg and the orders table is 40 meg.

## Project Structure

### Connection Modules (Top Level)
- `snowflake_connection.py` - Abstracts Snowflake database connections
- `clickhouse_connection.py` - Abstracts ClickHouse database connections
- Other utility scripts for data management and exploration

### Load Testing (loadtesting/ directory)
- `shared_loadtest.py` - Core load testing framework and client driver
- `snowflake_loadtest.py` - Snowflake-specific load tests  
- `clickhouse_loadtest.py` - ClickHouse-specific load tests
- `generate_graphs_from_csv.py` - Generate performance graphs from CSV results
- `compare_loadtest_results.py` - Compare results between databases
- CSV files with load test results
- PNG files with performance graphs

## How It Works

The load testing framework is pretty simplistic: it fires off a bunch of threads that wake up every 1/qps seconds and send a request. The rest is tabulating and plotting graphs.

## Results

[lineitem](https://github.com/ramaswamy11703/snowflake_github/blob/main/loadtesting/loadtest_comparison_lineitem.png) is the lineitem comparison across snowflake & clickhouse.

[orders](https://github.com/ramaswamy11703/snowflake_github/blob/main/loadtesting/loadtest_comparison_orders.png) is the orders table comparison.

## Running Load Tests

### Option 1: Using the convenience script (recommended)

From the top-level directory:

```bash
python3 run_loadtest.py snowflake          # Run Snowflake load tests
python3 run_loadtest.py clickhouse         # Run ClickHouse load tests
python3 run_loadtest.py graphs --all       # Generate graphs from existing results
python3 run_loadtest.py graphs --info      # Show CSV file info
python3 run_loadtest.py compare             # Compare results between databases
```

### Option 2: Navigate to loadtesting directory

```bash
cd loadtesting/
python3 snowflake_loadtest.py              # Run Snowflake load tests
python3 clickhouse_loadtest.py             # Run ClickHouse load tests
python3 generate_graphs_from_csv.py --all  # Generate graphs from existing results
python3 compare_loadtest_results.py        # Compare results between databases
```


