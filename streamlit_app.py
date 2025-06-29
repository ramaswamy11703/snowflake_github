#!/usr/bin/env python3
"""
Snowflake Database Explorer - Streamlit App

A simple Streamlit application that connects to Snowflake and displays
all available databases. This app can run locally and also in Snowflake
via Snowflake-GitHub integration.
"""

import streamlit as st
import pandas as pd
import os

# Only import local connection module if not running in Snowflake
try:
    # Check if we're running in Snowflake environment
    if 'SNOWFLAKE_ACCOUNT' not in os.environ:
        from snowflake_connection import connect_to_snowflake
    else:
        connect_to_snowflake = None
except ImportError:
    # If import fails, we're likely in Snowflake environment
    connect_to_snowflake = None

# Set page config
st.set_page_config(
    page_title="Snowflake Database Explorer",
    page_icon="❄️",
    layout="wide"
)

def get_snowflake_connection():
    """
    Get Snowflake connection - handles both local and Snowflake environments
    """
    try:
        # First try to use Snowflake's built-in connection (when running in Snowflake)
        try:
            st.info("Attempting to use st.connection('snowflake')")
            conn = st.connection('snowflake')
            st.success("Successfully created st.connection('snowflake')")
            return conn
        except Exception as e1:
            st.warning(f"st.connection('snowflake') failed: {e1}")
        
        # If that fails, try the experimental connection
        try:
            st.info("Attempting to use st.experimental_connection('snowflake')")
            conn = st.experimental_connection('snowflake')
            st.success("Successfully created st.experimental_connection('snowflake')")
            return conn
        except Exception as e2:
            st.warning(f"st.experimental_connection('snowflake') failed: {e2}")
        
        # Try without specifying connection name
        try:
            st.info("Attempting to use st.connection() without name")
            conn = st.connection()
            st.success("Successfully created st.connection()")
            return conn
        except Exception as e3:
            st.warning(f"st.connection() failed: {e3}")
            
        # If still no luck and we have our local connection module, use it
        if connect_to_snowflake is not None:
            st.info("Attempting to use local connection module")
            config_path = "/Users/srramaswamy/.snowsql/config"
            conn = connect_to_snowflake("my_conn", config_path)
            st.success("Successfully created local connection")
            return conn
        else:
            raise Exception("No connection method available - all attempts failed")
            
    except Exception as e:
        st.error(f"Connection error: {e}")
        import traceback
        st.error(f"Traceback: {traceback.format_exc()}")
        return None

def get_databases(conn):
    """
    Get list of all databases from Snowflake
    """
    try:
        # Debug: Show connection type
        st.info(f"Connection type: {type(conn)}")
        
        if hasattr(conn, 'query'):
            # Snowflake built-in connection (when running in Snowflake)
            st.info("Using Snowflake built-in connection")
            try:
                result = conn.query("SHOW DATABASES")
                st.info(f"Query result type: {type(result)}")
                st.info(f"Query result shape: {result.shape if hasattr(result, 'shape') else 'No shape attribute'}")
                if hasattr(result, 'columns'):
                    st.info(f"Query result columns: {list(result.columns)}")
                return result
            except Exception as query_error:
                st.error(f"Query error with built-in connection: {query_error}")
                # Try alternative query methods
                try:
                    # First try with correct INFORMATION_SCHEMA columns
                    result = conn.query("SELECT DATABASE_NAME, DATABASE_OWNER, COMMENT FROM INFORMATION_SCHEMA.DATABASES ORDER BY DATABASE_NAME")
                    st.info("✅ Used INFORMATION_SCHEMA.DATABASES successfully")
                    return result
                except Exception as alt_error1:
                    st.warning(f"INFORMATION_SCHEMA query failed: {alt_error1}")
                    try:
                        # Try even simpler query
                        result = conn.query("SELECT DATABASE_NAME FROM INFORMATION_SCHEMA.DATABASES ORDER BY DATABASE_NAME")
                        st.info("✅ Used simple INFORMATION_SCHEMA query successfully")
                        return result
                    except Exception as alt_error2:
                        st.error(f"Simple query also failed: {alt_error2}")
                        try:
                            # Last resort: try SHOW DATABASES with different approach
                            result = conn.query("SHOW DATABASES;")
                            st.info("✅ SHOW DATABASES with semicolon worked")
                            return result
                        except Exception as final_error:
                            st.error(f"All query methods failed: {final_error}")
                            return None
        else:
            # Regular snowflake-connector connection (when running locally)
            st.info("Using regular snowflake-connector connection")
            cursor = conn.cursor()
            cursor.execute("SHOW DATABASES")
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            cursor.close()
            return pd.DataFrame(data, columns=columns)
    except Exception as e:
        st.error(f"Error fetching databases: {str(e)}")
        st.error(f"Error type: {type(e)}")
        # Show more detailed error information
        import traceback
        st.error(f"Traceback: {traceback.format_exc()}")
        return None

def main():
    """
    Main Streamlit application
    """
    # Header
    st.title("❄️ Snowflake Database Explorer")
    st.markdown("---")
    
    # Welcome message
    st.markdown("""
    ### Hello World! 👋
    
    Welcome to the Snowflake Database Explorer! This simple app demonstrates:
    - 🔗 Connecting to Snowflake from Streamlit
    - 📊 Querying and displaying database information
    - 🌐 Running both locally and in Snowflake
    """)
    
    # Connection status
    with st.spinner("Connecting to Snowflake..."):
        conn = get_snowflake_connection()
    
    if conn is None:
        st.error("❌ Failed to connect to Snowflake")
        st.info("Please check your connection configuration.")
        return
    
    st.success("✅ Successfully connected to Snowflake!")
    
    # Test the connection with a simple query first
    st.markdown("### 🔍 Testing Connection")
    try:
        if hasattr(conn, 'query'):
            test_result = conn.query("SELECT CURRENT_USER(), CURRENT_ACCOUNT(), CURRENT_WAREHOUSE()")
            st.success("✅ Connection test successful!")
            st.info(f"Current User: {test_result.iloc[0, 0] if not test_result.empty else 'Unknown'}")
            st.info(f"Current Account: {test_result.iloc[0, 1] if not test_result.empty else 'Unknown'}")
            st.info(f"Current Warehouse: {test_result.iloc[0, 2] if not test_result.empty else 'Unknown'}")
        else:
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT_USER(), CURRENT_ACCOUNT(), CURRENT_WAREHOUSE()")
            result = cursor.fetchone()
            cursor.close()
            st.success("✅ Connection test successful!")
            st.info(f"Current User: {result[0]}")
            st.info(f"Current Account: {result[1]}")
            st.info(f"Current Warehouse: {result[2]}")
    except Exception as test_error:
        st.error(f"❌ Connection test failed: {test_error}")
    
    # Fetch and display databases
    st.markdown("### 📚 Available Databases")
    
    with st.spinner("Fetching database list..."):
        databases_df = get_databases(conn)
    
    if databases_df is not None and not databases_df.empty:
        st.markdown(f"**Found {len(databases_df)} databases:**")
        
        # Display as a nice table
        st.dataframe(
            databases_df,
            use_container_width=True,
            hide_index=True
        )
        
        # Show some statistics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Databases", len(databases_df))
        
        with col2:
            # Count databases by type if TYPE column exists
            if 'TYPE' in databases_df.columns:
                standard_count = len(databases_df[databases_df['TYPE'] == 'STANDARD'])
                st.metric("Standard Databases", standard_count)
            else:
                st.metric("Visible Databases", len(databases_df))
        
        with col3:
            # Show if any sample databases are present
            # Check for different possible column names
            name_column = None
            for col in ['NAME', 'DATABASE_NAME', 'name', 'database_name']:
                if col in databases_df.columns:
                    name_column = col
                    break
            
            if name_column:
                sample_count = len(databases_df[databases_df[name_column].str.contains('SAMPLE', case=False, na=False)])
                st.metric("Sample Databases", sample_count)
            else:
                st.metric("Database Columns", len(databases_df.columns))
        
        # Show column information for debugging
        st.info(f"Available columns: {list(databases_df.columns)}")
        
        # Expandable section with raw data
        with st.expander("🔍 View Raw Database Information"):
            st.json(databases_df.to_dict('records'))
    
    else:
        st.warning("⚠️ No databases found or error occurred while fetching data.")
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666;'>
        <p>Built with ❤️ using Streamlit and Snowflake</p>
        <p>Ready for local development and Snowflake deployment! 🚀</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Close connection if it's a regular connection (not Snowflake built-in)
    if conn and not hasattr(conn, 'query'):
        conn.close()

if __name__ == "__main__":
    main() 