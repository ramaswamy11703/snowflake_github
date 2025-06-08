# snowflake_github

Load test scripts to compare clickhouse / snowflake performance.

This is on a 1% sample of tpc-h.

The lineitem table is roughly 160meg and the orders table is 40 meg.

Roughly speaking `snowflake_connection.py` and `clickhouse_connection.py` abstract how we connect to these sources

`shared_loadtest.py` is the client driver. It is pretty simplistic: it fires off a bunch of threads that wake up every 1/qps seconds and send a request. 

The rest is tabulating and plotting graphs.

[lineitem](https://github.com/ramaswamy11703/snowflake_github/edit/main/loatest_comparison_lineitem.png) is the lineitem comparison across snowflake & clickhouse.

[orders](https://github.com/ramaswamy11703/snowflake_github/edit/main/loatest_comparison_orders.png) is the orders table comparison.


