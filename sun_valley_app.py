#!/usr/bin/env python3
"""
Sun Valley App - Streamlit Application

A Streamlit application that connects to Snowflake and operates in the context
of the "sun_valley" database and "y2025" schema. This app can run locally 
and also in Snowflake via Snowflake-GitHub integration.
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
    page_title="Sun Valley App",
    page_icon="🏔️",
    layout="wide"
)

# Default database and schema
DEFAULT_DATABASE = "sun_valley"
DEFAULT_SCHEMA = "y2025"

def is_running_in_snowflake():
    """
    Detect if we're running inside Snowflake environment
    """
    # Check for Snowflake environment indicators
    snowflake_indicators = [
        'SNOWFLAKE_ACCOUNT' in os.environ,
        'SNOWFLAKE_USER' in os.environ,
        'SNOWFLAKE_ROLE' in os.environ,
        'SNOWFLAKE_WAREHOUSE' in os.environ,
        'SNOWFLAKE_DATABASE' in os.environ,
        'SNOWFLAKE_SCHEMA' in os.environ
    ]
    
    return any(snowflake_indicators)

def get_snowflake_connection():
    """
    Get Snowflake connection - handles both local and Snowflake environments
    """
    try:
        # First try to use Snowflake's built-in connection (when running in Snowflake)
        try:
            return st.connection('snowflake')
        except Exception as e1:
            pass
        
        # If that fails, try the experimental connection
        try:
            return st.experimental_connection('snowflake')
        except Exception as e2:
            pass
        
        # Try without specifying connection name
        try:
            return st.connection()
        except Exception as e3:
            pass
            
        # If still no luck and we have our local connection module, use it
        if connect_to_snowflake is not None:
            config_path = "/Users/srramaswamy/.snowsql/config"
            return connect_to_snowflake("my_conn", config_path)
        else:
            raise Exception("No connection method available")
            
    except Exception as e:
        st.error(f"Connection error: {e}")
        import traceback
        st.error(f"Traceback: {traceback.format_exc()}")
        return None

def set_database_context(conn, database=DEFAULT_DATABASE, schema=DEFAULT_SCHEMA):
    """
    Set the database and schema context for the connection
    """
    try:
        st.info(f"🔧 Setting database context to {database}.{schema}")
        
        if hasattr(conn, 'query'):
            # Snowflake built-in connection (when running in Snowflake)
            st.info("Using Snowflake built-in connection for context setting...")
            try:
                conn.query(f"USE DATABASE {database}")
                st.success(f"✅ Database set to: {database}")
                conn.query(f"USE SCHEMA {schema}")
                st.success(f"✅ Schema set to: {schema}")
                return True
            except Exception as snowflake_error:
                st.warning(f"⚠️ Could not set context in Snowflake environment: {snowflake_error}")
                st.info("This is normal when running inside Snowflake with limited privileges")
                st.info("Continuing with current context...")
                return True  # Continue anyway, as context might already be set
        else:
            # Regular snowflake-connector connection (when running locally)
            st.info("Using regular snowflake-connector connection for context setting...")
            cursor = conn.cursor()
            
            # Check if database exists (with privilege handling)
            st.info(f"Checking if database '{database}' exists...")
            try:
                cursor.execute("SHOW DATABASES")
                databases = cursor.fetchall()
                db_names = [db[1] for db in databases]  # Database name is usually in column 1
                st.info(f"Available databases: {db_names}")
                
                if database.upper() not in [db.upper() for db in db_names]:
                    st.error(f"❌ Database '{database}' not found in available databases!")
                    cursor.close()
                    return False
            except Exception as db_check_error:
                st.warning(f"⚠️ Could not check available databases: {db_check_error}")
                st.info("Attempting to use database anyway...")
            
            try:
                cursor.execute(f"USE DATABASE {database}")
                st.success(f"✅ Database set to: {database}")
            except Exception as db_use_error:
                st.error(f"❌ Could not use database {database}: {db_use_error}")
                cursor.close()
                return False
            
            # Check if schema exists (with privilege handling)
            st.info(f"Checking if schema '{schema}' exists in database '{database}'...")
            try:
                cursor.execute("SHOW SCHEMAS")
                schemas = cursor.fetchall()
                schema_names = [sch[1] for sch in schemas]  # Schema name is usually in column 1
                st.info(f"Available schemas in {database}: {schema_names}")
                
                if schema.upper() not in [sch.upper() for sch in schema_names]:
                    st.error(f"❌ Schema '{schema}' not found in available schemas!")
                    cursor.close()
                    return False
            except Exception as schema_check_error:
                st.warning(f"⚠️ Could not check available schemas: {schema_check_error}")
                st.info("Attempting to use schema anyway...")
            
            try:
                cursor.execute(f"USE SCHEMA {schema}")
                st.success(f"✅ Schema set to: {schema}")
            except Exception as schema_use_error:
                st.error(f"❌ Could not use schema {schema}: {schema_use_error}")
                cursor.close()
                return False
            
            cursor.close()
            return True
    except Exception as e:
        st.warning(f"⚠️ Error setting database context: {e}")
        st.info("Continuing with current context...")
        return True  # Continue anyway to avoid blocking the app

def get_tables_in_schema(conn, database=DEFAULT_DATABASE, schema=DEFAULT_SCHEMA):
    """
    Get list of tables in the specified schema
    """
    try:
        # Log the connection type and query details
        st.info(f"🔍 Fetching tables from {database}.{schema}")
        st.info(f"Connection type: {'Snowflake built-in' if hasattr(conn, 'query') else 'Regular snowflake-connector'}")
        
        if hasattr(conn, 'query'):
            # Snowflake built-in connection (when running in Snowflake)
            st.info("Using Snowflake built-in connection...")
            
            # Try multiple approaches for Snowflake environment
            approaches = [
                # Approach 1: Simple SHOW TABLES (most likely to work in Snowflake)
                ("SHOW TABLES", "SHOW TABLES"),
                
                # Approach 2: INFORMATION_SCHEMA with current context
                ("INFORMATION_SCHEMA (current context)", 
                 "SELECT TABLE_NAME, TABLE_TYPE, ROW_COUNT, BYTES, CREATED, LAST_ALTERED, COMMENT FROM INFORMATION_SCHEMA.TABLES ORDER BY TABLE_NAME"),
                
                # Approach 3: INFORMATION_SCHEMA with explicit schema filter
                ("INFORMATION_SCHEMA (explicit schema)", 
                 f"SELECT TABLE_NAME, TABLE_TYPE, ROW_COUNT, BYTES, CREATED, LAST_ALTERED, COMMENT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema.upper()}' ORDER BY TABLE_NAME"),
                
                # Approach 4: INFORMATION_SCHEMA with database prefix
                ("INFORMATION_SCHEMA (database prefix)", 
                 f"SELECT TABLE_NAME, TABLE_TYPE, ROW_COUNT, BYTES, CREATED, LAST_ALTERED, COMMENT FROM {database}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema.upper()}' ORDER BY TABLE_NAME")
            ]
            
            for approach_name, query in approaches:
                try:
                    st.info(f"Trying approach: {approach_name}")
                    st.code(f"Query: {query}")
                    
                    result = conn.query(query)
                    st.success(f"✅ {approach_name} succeeded!")
                    
                    if hasattr(result, 'shape'):
                        st.info(f"Result shape: {result.shape}")
                        if result.shape[0] > 0:
                            st.success(f"Found {result.shape[0]} tables!")
                            return result
                        else:
                            st.warning(f"Query succeeded but returned 0 tables")
                    else:
                        st.info(f"Result type: {type(result)}")
                        if len(result) > 0:
                            st.success(f"Found {len(result)} tables!")
                            return result
                        else:
                            st.warning(f"Query succeeded but returned 0 tables")
                            
                except Exception as approach_error:
                    st.warning(f"❌ {approach_name} failed: {approach_error}")
                    continue
            
            # If all approaches failed, return empty DataFrame
            st.warning("All Snowflake approaches failed, returning empty result")
            return pd.DataFrame(columns=['TABLE_NAME', 'TABLE_TYPE', 'ROW_COUNT', 'BYTES', 'CREATED', 'LAST_ALTERED', 'COMMENT'])
            
        else:
            # Regular snowflake-connector connection (when running locally)
            st.info("Using regular snowflake-connector connection...")
            cursor = conn.cursor()
            
            # First, let's check current context
            cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
            current_context = cursor.fetchone()
            st.info(f"Current context: Database={current_context[0]}, Schema={current_context[1]}")
            
            # Try a simpler query first
            st.info("Trying SHOW TABLES first...")
            show_tables_result = []
            try:
                cursor.execute("SHOW TABLES")
                show_tables_result = cursor.fetchall()
                st.info(f"SHOW TABLES returned {len(show_tables_result)} tables")
                for table in show_tables_result:
                    st.write(f"  - {table[1] if len(table) > 1 else table[0]}")
            except Exception as show_error:
                st.warning(f"SHOW TABLES failed: {show_error}")
            
            # If SHOW TABLES found tables, use that data instead of INFORMATION_SCHEMA
            if show_tables_result:
                st.info("Using SHOW TABLES results since they were found...")
                # Convert SHOW TABLES result to DataFrame with expected columns
                show_columns = [desc[0] for desc in cursor.description]
                st.info(f"SHOW TABLES columns: {show_columns}")
                
                # Create a DataFrame with the expected structure
                table_data = []
                for row in show_tables_result:
                    table_data.append({
                        'TABLE_NAME': row[1] if len(row) > 1 else row[0],  # Table name
                        'TABLE_TYPE': 'BASE TABLE',  # Default type
                        'ROW_COUNT': None,  # Not available in SHOW TABLES
                        'BYTES': None,  # Not available in SHOW TABLES
                        'CREATED': row[0] if len(row) > 0 else None,  # Created date might be first column
                        'LAST_ALTERED': None,  # Not available in SHOW TABLES
                        'COMMENT': row[6] if len(row) > 6 else None  # Comment might be in column 6
                    })
                
                result_df = pd.DataFrame(table_data)
                st.success(f"Successfully created DataFrame from SHOW TABLES with shape: {result_df.shape}")
                cursor.close()
                return result_df
            
            # If SHOW TABLES didn't work, try the INFORMATION_SCHEMA query
            st.info("SHOW TABLES didn't find tables, trying INFORMATION_SCHEMA query...")
            
            # Try different variations of the schema name
            schema_variations = [schema, schema.upper(), schema.lower()]
            for schema_var in schema_variations:
                try:
                    test_query = f"""
                    SELECT 
                        TABLE_NAME,
                        TABLE_TYPE,
                        ROW_COUNT,
                        BYTES,
                        CREATED,
                        LAST_ALTERED,
                        COMMENT
                    FROM {database}.INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = '{schema_var}'
                    ORDER BY TABLE_NAME
                    """
                    
                    st.info(f"Trying schema variation: '{schema_var}'")
                    cursor.execute(test_query)
                    test_data = cursor.fetchall()
                    st.info(f"Schema '{schema_var}' returned {len(test_data)} rows")
                    
                    if test_data:
                        columns = [desc[0] for desc in cursor.description]
                        result_df = pd.DataFrame(test_data, columns=columns)
                        st.success(f"Found tables with schema '{schema_var}'!")
                        cursor.close()
                        return result_df
                        
                except Exception as var_error:
                    st.warning(f"Schema variation '{schema_var}' failed: {var_error}")
            
            # If all variations failed, return empty DataFrame
            st.warning("All schema variations failed, returning empty result")
            cursor.close()
            return pd.DataFrame(columns=['TABLE_NAME', 'TABLE_TYPE', 'ROW_COUNT', 'BYTES', 'CREATED', 'LAST_ALTERED', 'COMMENT'])
            
    except Exception as e:
        st.error(f"Error fetching tables: {str(e)}")
        st.error(f"Error type: {type(e)}")
        import traceback
        st.error(f"Full traceback:\n{traceback.format_exc()}")
        return None

def get_table_preview(conn, table_name, database=DEFAULT_DATABASE, schema=DEFAULT_SCHEMA, limit=10):
    """
    Get a preview of data from a specific table
    """
    try:
        query = f"SELECT * FROM {database}.{schema}.{table_name} LIMIT {limit}"
        
        if hasattr(conn, 'query'):
            # Snowflake built-in connection (when running in Snowflake)
            result = conn.query(query)
            return result
        else:
            # Regular snowflake-connector connection (when running locally)
            cursor = conn.cursor()
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            cursor.close()
            return pd.DataFrame(data, columns=columns)
    except Exception as e:
        st.error(f"Error fetching table preview: {str(e)}")
        return None

def get_column_info(conn, table_name, database=DEFAULT_DATABASE, schema=DEFAULT_SCHEMA):
    """
    Get column information for a specific table
    """
    try:
        query = f"""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            COMMENT
        FROM {database}.INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """
        
        if hasattr(conn, 'query'):
            # Snowflake built-in connection (when running in Snowflake)
            result = conn.query(query)
            return result
        else:
            # Regular snowflake-connector connection (when running locally)
            cursor = conn.cursor()
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            cursor.close()
            return pd.DataFrame(data, columns=columns)
    except Exception as e:
        st.error(f"Error fetching column info: {str(e)}")
        return None

def main():
    """
    Main Streamlit application
    """
    # Header
    st.title("🏔️ Sun Valley App")
    st.markdown("---")
    
    # Environment detection and welcome message
    in_snowflake = is_running_in_snowflake()
    environment = "Snowflake Cloud" if in_snowflake else "Local Development"
    
    st.markdown(f"""
    ### Welcome to Sun Valley! 🎿
    
    **Environment**: {environment} {'☁️' if in_snowflake else '💻'}
    
    This app operates in the context of:
    - **Database**: `{DEFAULT_DATABASE}`
    - **Schema**: `{DEFAULT_SCHEMA}`
    
    Features:
    - 🔗 Connects to Snowflake (local and cloud)
    - 📊 Explores tables and data
    - 🔍 Provides table previews and column information
    - 🌐 Runs both locally and in Snowflake
    
    {f"**Note**: Running in Snowflake environment with potentially limited privileges" if in_snowflake else ""}
    """)
    
    # Connection status
    with st.spinner("Connecting to Snowflake..."):
        conn = get_snowflake_connection()
    
    if conn is None:
        st.error("❌ Failed to connect to Snowflake")
        st.info("Please check your connection configuration.")
        return
    
    st.success("✅ Successfully connected to Snowflake!")
    
    # Set database context (with privilege-aware handling)
    with st.spinner(f"Setting context to {DEFAULT_DATABASE}.{DEFAULT_SCHEMA}..."):
        context_set = set_database_context(conn, DEFAULT_DATABASE, DEFAULT_SCHEMA)
    
    if not context_set:
        st.warning(f"⚠️ Could not explicitly set context to {DEFAULT_DATABASE}.{DEFAULT_SCHEMA}")
        st.info("This might be due to privilege limitations in the Snowflake environment.")
        st.info("Continuing with current context - the app may still work if you already have access to the required objects.")
        # Don't return here - continue with the app
    else:
        st.success(f"✅ Context set to {DEFAULT_DATABASE}.{DEFAULT_SCHEMA}")
    
    # Show connection info
    try:
        if hasattr(conn, 'query'):
            test_result = conn.query("SELECT CURRENT_USER(), CURRENT_ACCOUNT(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
            if not test_result.empty:
                st.info(f"Connected as: **{test_result.iloc[0, 0]}** | Account: **{test_result.iloc[0, 1]}** | Warehouse: **{test_result.iloc[0, 2]}** | Database: **{test_result.iloc[0, 3]}** | Schema: **{test_result.iloc[0, 4]}**")
        else:
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT_USER(), CURRENT_ACCOUNT(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
            result = cursor.fetchone()
            cursor.close()
            st.info(f"Connected as: **{result[0]}** | Account: **{result[1]}** | Warehouse: **{result[2]}** | Database: **{result[3]}** | Schema: **{result[4]}**")
    except Exception as test_error:
        pass  # Don't show errors for connection info
    
    # Main content area
    st.markdown("### 📚 Tables in Sun Valley Schema")
    
    # Fetch and display tables
    with st.spinner("Fetching table list..."):
        tables_df = get_tables_in_schema(conn, DEFAULT_DATABASE, DEFAULT_SCHEMA)
    
    if tables_df is not None and not tables_df.empty:
        st.markdown(f"**Found {len(tables_df)} tables in {DEFAULT_DATABASE}.{DEFAULT_SCHEMA}:**")
        
        # Display tables in a nice format
        st.dataframe(
            tables_df,
            use_container_width=True,
            hide_index=True
        )
        
        # Show some statistics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Tables", len(tables_df))
        
        with col2:
            if 'TABLE_TYPE' in tables_df.columns:
                table_count = len(tables_df[tables_df['TABLE_TYPE'] == 'BASE TABLE'])
                st.metric("Base Tables", table_count)
            else:
                st.metric("Visible Tables", len(tables_df))
        
        with col3:
            if 'ROW_COUNT' in tables_df.columns:
                total_rows = tables_df['ROW_COUNT'].sum()
                st.metric("Total Rows", f"{total_rows:,}")
            else:
                st.metric("Table Columns", len(tables_df.columns))
        
        # Table explorer section
        st.markdown("### 🔍 Table Explorer")
        
        # Select a table to explore
        table_names = tables_df['TABLE_NAME'].tolist() if 'TABLE_NAME' in tables_df.columns else []
        
        if table_names:
            selected_table = st.selectbox("Select a table to explore:", table_names)
            
            if selected_table:
                # Create tabs for different views
                tab1, tab2, tab3 = st.tabs(["📊 Data Preview", "🏗️ Column Info", "📋 Table Details"])
                
                with tab1:
                    st.markdown(f"#### Data Preview: {selected_table}")
                    preview_limit = st.slider("Number of rows to preview:", 5, 100, 10)
                    
                    with st.spinner("Fetching data preview..."):
                        preview_df = get_table_preview(conn, selected_table, DEFAULT_DATABASE, DEFAULT_SCHEMA, preview_limit)
                    
                    if preview_df is not None and not preview_df.empty:
                        st.dataframe(preview_df, use_container_width=True)
                        st.info(f"Showing first {len(preview_df)} rows of {selected_table}")
                    else:
                        st.warning("No data found or error occurred while fetching preview.")
                
                with tab2:
                    st.markdown(f"#### Column Information: {selected_table}")
                    
                    with st.spinner("Fetching column information..."):
                        columns_df = get_column_info(conn, selected_table, DEFAULT_DATABASE, DEFAULT_SCHEMA)
                    
                    if columns_df is not None and not columns_df.empty:
                        st.dataframe(columns_df, use_container_width=True, hide_index=True)
                        st.info(f"Table {selected_table} has {len(columns_df)} columns")
                    else:
                        st.warning("No column information found or error occurred.")
                
                with tab3:
                    st.markdown(f"#### Table Details: {selected_table}")
                    
                    # Show detailed info for the selected table
                    table_info = tables_df[tables_df['TABLE_NAME'] == selected_table]
                    if not table_info.empty:
                        for col in table_info.columns:
                            if col in ['ROW_COUNT', 'BYTES']:
                                value = table_info[col].iloc[0]
                                if pd.notna(value):
                                    if col == 'BYTES':
                                        # Convert bytes to human readable format
                                        if value >= 1024**3:
                                            formatted_value = f"{value / (1024**3):.2f} GB"
                                        elif value >= 1024**2:
                                            formatted_value = f"{value / (1024**2):.2f} MB"
                                        elif value >= 1024:
                                            formatted_value = f"{value / 1024:.2f} KB"
                                        else:
                                            formatted_value = f"{value} bytes"
                                        st.metric(col.replace('_', ' ').title(), formatted_value)
                                    else:
                                        st.metric(col.replace('_', ' ').title(), f"{value:,}")
                            elif col in ['CREATED', 'LAST_ALTERED']:
                                value = table_info[col].iloc[0]
                                if pd.notna(value):
                                    st.write(f"**{col.replace('_', ' ').title()}:** {value}")
                            elif col == 'COMMENT':
                                value = table_info[col].iloc[0]
                                if pd.notna(value) and value.strip():
                                    st.write(f"**Comment:** {value}")
        
        # Expandable section with raw table data
        with st.expander("🔍 View Raw Table Information"):
            st.json(tables_df.to_dict('records'))
    
    else:
        st.warning(f"⚠️ No tables found in {DEFAULT_DATABASE}.{DEFAULT_SCHEMA} or error occurred while fetching data.")
        st.info("Please ensure the database and schema exist and contain tables.")
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666;'>
        <p>🏔️ Sun Valley App - Built with ❤️ using Streamlit and Snowflake</p>
        <p>Ready for local development and Snowflake deployment! 🚀</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Close connection if it's a regular connection (not Snowflake built-in)
    if conn and not hasattr(conn, 'query'):
        conn.close()

if __name__ == "__main__":
    main() 