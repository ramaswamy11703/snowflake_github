#!/usr/bin/env python3
"""
Test Snowflake Connection

This script tests the Snowflake connection functionality to ensure
it's working correctly after the directory reorganization.
"""

import sys
from snowflake_connection import connect_to_snowflake, test_connection, get_connection_params

def test_basic_connection():
    """
    Test basic Snowflake connection functionality
    """
    print("🔗 Testing basic Snowflake connection...")
    
    config_path = "/Users/srramaswamy/.snowsql/config"
    
    try:
        conn = connect_to_snowflake("snowhouse", config_path)
        if conn:
            print("✅ Connection established successfully!")
            
            # Test the connection
            if test_connection(conn):
                print("✅ Connection test passed!")
                conn.close()
                return True
            else:
                print("❌ Connection test failed!")
                conn.close()
                return False
        else:
            print("❌ Failed to establish connection!")
            return False
            
    except Exception as e:
        print(f"❌ Error during connection test: {e}")
        return False

def test_connection_params():
    """
    Test getting connection parameters for both connections
    """
    print("\n🔧 Testing connection parameter extraction...")

    config_path = "/Users/srramaswamy/.snowsql/config"

    try:
        # Test snowhouse connection (password auth)
        print("\n   Testing snowhouse connection parameters:")
        params_snowhouse = get_connection_params(config_path, "snowhouse")
        if params_snowhouse:
            print("   ✅ Snowhouse parameters extracted successfully!")
            print(f"      Account: {params_snowhouse.get('account', 'N/A')}")
            print(f"      User: {params_snowhouse.get('user', 'N/A')}")
            print(f"      Private Key: {'Present' if params_snowhouse.get('private_key') else 'Missing'}")
            print(f"      Password: {'Present' if params_snowhouse.get('password') else 'Missing'}")
        else:
            print("   ❌ Failed to extract snowhouse parameters!")
            return False

        # Test my_conn connection (JWT auth)
        print("\n   Testing my_conn connection parameters:")
        params_my_conn = get_connection_params(config_path, "my_conn")
        if params_my_conn:
            print("   ✅ My_conn parameters extracted successfully!")
            print(f"      Account: {params_my_conn.get('account', 'N/A')}")
            print(f"      User: {params_my_conn.get('user', 'N/A')}")
            print(f"      Private Key: {'Present' if params_my_conn.get('private_key') else 'Missing'}")
            print(f"      Password: {'Present' if params_my_conn.get('password') else 'Missing'}")
            return True
        else:
            print("   ❌ Failed to extract my_conn parameters!")
            return False

    except Exception as e:
        print(f"❌ Error extracting connection parameters: {e}")
        return False

def test_warehouse_access():
    """
    Test accessing the loadtest warehouse
    """
    print("\n🏭 Testing warehouse access...")
    
    config_path = "/Users/srramaswamy/.snowsql/config"
    
    try:
        conn = connect_to_snowflake("my_conn", config_path)
        if conn:
            cursor = conn.cursor()
            
            # Try to use the loadtest warehouse
            cursor.execute("USE WAREHOUSE loadtest")
            print("✅ Successfully switched to loadtest warehouse!")
            
            # Test a simple query
            cursor.execute("SELECT CURRENT_WAREHOUSE()")
            result = cursor.fetchone()
            print(f"   Current warehouse: {result[0]}")
            
            cursor.close()
            conn.close()
            return True
        else:
            print("❌ Failed to establish connection for warehouse test!")
            return False
            
    except Exception as e:
        print(f"❌ Error during warehouse test: {e}")
        return False

def test_sample_data_access():
    """
    Test accessing the SNOWFLAKE_SAMPLE_DATA
    """
    print("\n📊 Testing sample data access...")
    
    config_path = "/Users/srramaswamy/.snowsql/config"
    
    try:
        conn = connect_to_snowflake("my_conn", config_path)
        if conn:
            cursor = conn.cursor()
            cursor.execute("USE WAREHOUSE loadtest")
            
            # Test accessing the sample data
            cursor.execute("""
                SELECT COUNT(*) 
                FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS 
                LIMIT 1
            """)
            result = cursor.fetchone()
            print(f"✅ Successfully accessed ORDERS table! Row count: {result[0]:,}")
            
            cursor.execute("""
                SELECT COUNT(*) 
                FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM 
                LIMIT 1
            """)
            result = cursor.fetchone()
            print(f"✅ Successfully accessed LINEITEM table! Row count: {result[0]:,}")
            
            cursor.close()
            conn.close()
            return True
        else:
            print("❌ Failed to establish connection for sample data test!")
            return False
            
    except Exception as e:
        print(f"❌ Error during sample data test: {e}")
        return False

def test_snowhouse_data_access():
    """
    Test accessing the office_of_ceo database and use_cases schema in snowhouse
    """
    print("\n🏢 Testing snowhouse database access...")

    config_path = "/Users/srramaswamy/.snowsql/config"

    try:
        conn = connect_to_snowflake("snowhouse", config_path)
        if conn:
            cursor = conn.cursor()

            # Switch to the office_of_ceo database and use_cases schema
            cursor.execute("USE DATABASE office_of_ceo")
            print("✅ Successfully switched to office_of_ceo database!")

            cursor.execute("USE SCHEMA use_cases")
            print("✅ Successfully switched to use_cases schema!")

            # List all tables in the schema
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()

            if tables:
                print(f"✅ Found {len(tables)} table(s) in office_of_ceo.use_cases:")
                for table in tables:
                    table_name = table[1]  # Table name is typically in the second column
                    print(f"   📋 {table_name}")

                    # Get row count for each table
                    try:
                        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                        count_result = cursor.fetchone()
                        row_count = count_result[0] if count_result else 0
                        print(f"      Rows: {row_count:,}")
                    except Exception as e:
                        print(f"      Rows: Unable to count ({str(e)[:50]}...)")
            else:
                print("⚠️  No tables found in office_of_ceo.use_cases schema")

            cursor.close()
            conn.close()
            return True
        else:
            print("❌ Failed to establish connection for snowhouse database test!")
            return False

    except Exception as e:
        print(f"❌ Error during snowhouse database test: {e}")
        return False

def main():
    """
    Main function to run all connection tests
    """
    print("🚀 Snowflake Connection Test Suite")
    print("=" * 50)
    
    tests = [
        ("Basic Connection (Snowhouse)", test_basic_connection),
        ("Connection Parameters (Both)", test_connection_params),
        ("Warehouse Access (My_Conn)", test_warehouse_access),
        ("Sample Data Access (My_Conn)", test_sample_data_access),
        ("Office of CEO Database Access (Snowhouse)", test_snowhouse_data_access)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n{'='*60}")
        print(f"🧪 Running Test: {test_name}")
        print(f"{'='*60}")
        
        if test_func():
            passed += 1
            print(f"✅ {test_name} - PASSED")
        else:
            print(f"❌ {test_name} - FAILED")
    
    print(f"\n{'='*60}")
    print(f"🎯 Test Results Summary")
    print(f"{'='*60}")
    print(f"Tests Passed: {passed}/{total}")
    print(f"Success Rate: {(passed/total)*100:.1f}%")
    
    if passed == total:
        print("🎉 All tests passed! Snowflake connection is working perfectly!")
        return True
    else:
        print(f"⚠️  {total-passed} test(s) failed. Please check the configuration.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 