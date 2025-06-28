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
        conn = connect_to_snowflake("my_conn", config_path)
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
    Test getting connection parameters for threading
    """
    print("\n🔧 Testing connection parameter extraction...")
    
    config_path = "/Users/srramaswamy/.snowsql/config"
    
    try:
        params = get_connection_params(config_path, "my_conn")
        if params:
            print("✅ Connection parameters extracted successfully!")
            print(f"   Account: {params.get('account', 'N/A')}")
            print(f"   User: {params.get('user', 'N/A')}")
            print(f"   Private Key: {'Present' if params.get('private_key') else 'Missing'}")
            return True
        else:
            print("❌ Failed to extract connection parameters!")
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

def main():
    """
    Main function to run all connection tests
    """
    print("🚀 Snowflake Connection Test Suite")
    print("=" * 50)
    
    tests = [
        ("Basic Connection", test_basic_connection),
        ("Connection Parameters", test_connection_params),
        ("Warehouse Access", test_warehouse_access),
        ("Sample Data Access", test_sample_data_access)
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