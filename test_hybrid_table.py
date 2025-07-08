#!/usr/bin/env python3
"""
Test script to verify the hybrid table structure and ID column
"""

import pandas as pd
from snowflake_connection import connect_to_snowflake

def test_hybrid_table():
    """
    Test the hybrid table structure and sample data
    """
    print("🔗 Connecting to Snowflake...")
    conn = connect_to_snowflake()
    
    if conn is None:
        print("❌ Failed to connect to Snowflake")
        return False
    
    try:
        cursor = conn.cursor()
        
        # Set the database and schema context
        print("🎯 Setting database context...")
        cursor.execute("USE DATABASE sun_valley")
        cursor.execute("USE SCHEMA y2025")
        
        # Check table structure
        print("🔍 Checking hybrid table structure...")
        cursor.execute("DESCRIBE TABLE SUNVALLEY_2025LIST_HYBRID")
        columns_info = cursor.fetchall()
        
        print("📋 Hybrid table columns:")
        for col in columns_info:
            print(f"  - {col[0]}: {col[1]} (nullable: {col[2]})")
        
        # Check sample data with ID column
        print("\n📊 Sample data with ID column:")
        cursor.execute("SELECT ID, NAME, STATUS FROM SUNVALLEY_2025LIST_HYBRID ORDER BY ID LIMIT 5")
        sample_data = cursor.fetchall()
        
        if sample_data:
            print("Sample records:")
            for row in sample_data:
                print(f"  ID: {row[0]}, NAME: {row[1]}, STATUS: {row[2]}")
        
        # Check if ID is auto-incrementing properly
        print("\n🔢 ID column statistics:")
        cursor.execute("SELECT MIN(ID), MAX(ID), COUNT(*) FROM SUNVALLEY_2025LIST_HYBRID")
        id_stats = cursor.fetchone()
        print(f"  Min ID: {id_stats[0]}, Max ID: {id_stats[1]}, Total records: {id_stats[2]}")
        
        # Test a specific record (like "Amodei, Dario")
        print("\n🎯 Testing specific record lookup:")
        cursor.execute("SELECT ID, NAME, STATUS FROM SUNVALLEY_2025LIST_HYBRID WHERE NAME LIKE '%Amodei%'")
        amodei_records = cursor.fetchall()
        
        if amodei_records:
            print("Found Amodei records:")
            for row in amodei_records:
                print(f"  ID: {row[0]}, NAME: {row[1]}, STATUS: {row[2]}")
        else:
            print("  No Amodei records found")
        
        # Test update query format
        print("\n🧪 Testing update query format:")
        test_id = id_stats[0]  # Use the minimum ID for testing
        test_query = f"""
        SELECT ID, NAME, STATUS FROM SUNVALLEY_2025LIST_HYBRID 
        WHERE ID = {test_id}
        """
        print(f"Test query: {test_query}")
        
        cursor.execute(test_query)
        test_record = cursor.fetchone()
        if test_record:
            print(f"Test record: ID={test_record[0]}, NAME={test_record[1]}, STATUS={test_record[2]}")
        
        cursor.close()
        return True
        
    except Exception as e:
        print(f"❌ Error testing hybrid table: {e}")
        import traceback
        print(f"Full traceback:\n{traceback.format_exc()}")
        return False
    
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    print("🏔️ Sun Valley Hybrid Table Test")
    print("=" * 50)
    
    success = test_hybrid_table()
    
    if success:
        print("\n✅ Hybrid table test completed!")
    else:
        print("\n❌ Hybrid table test failed!") 