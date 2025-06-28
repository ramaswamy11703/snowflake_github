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
            return st.connection('snowflake')
        except:
            pass
        
        # If that fails, try the experimental connection
        try:
            return st.experimental_connection('snowflake')
        except:
            pass
            
        # If still no luck and we have our local connection module, use it
        if connect_to_snowflake is not None:
            config_path = "/Users/srramaswamy/.snowsql/config"
            return connect_to_snowflake("my_conn", config_path)
        else:
            raise Exception("No connection method available")
            
    except Exception as e:
        st.error(f"Connection error: {e}")
        return None

def get_databases(conn):
    """
    Get list of all databases from Snowflake
    """
    try:
        if hasattr(conn, 'query'):
            # Snowflake built-in connection (when running in Snowflake)
            result = conn.query("SHOW DATABASES")
            return result
        else:
            # Regular snowflake-connector connection (when running locally)
            cursor = conn.cursor()
            cursor.execute("SHOW DATABASES")
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            cursor.close()
            return pd.DataFrame(data, columns=columns)
    except Exception as e:
        st.error(f"Error fetching databases: {e}")
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
            if 'NAME' in databases_df.columns:
                sample_count = len(databases_df[databases_df['NAME'].str.contains('SAMPLE', case=False, na=False)])
                st.metric("Sample Databases", sample_count)
        
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