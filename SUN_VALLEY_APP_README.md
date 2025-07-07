# 🏔️ Sun Valley App

A Streamlit application that connects to Snowflake and operates in the context of the `sun_valley` database and `y2025` schema. This app can run both locally and inside Snowflake.

## Features

- 🔗 **Dual Environment Support**: Runs locally and in Snowflake
- 📊 **Table Explorer**: Browse tables in the sun_valley.y2025 schema
- 🔍 **Data Preview**: View sample data from any table
- 🏗️ **Column Information**: Inspect table schemas and column details
- 📋 **Table Details**: View table metadata including row counts and sizes

## Default Context

The app automatically sets the following context:
- **Database**: `sun_valley`
- **Schema**: `y2025`

## Prerequisites

### For Local Development
- Python 3.8+
- Required packages (install via `pip install -r requirements.txt`):
  - `streamlit`
  - `snowflake-connector-python`
  - `pandas`
  - `cryptography`
- Snowflake connection configuration in `~/.snowsql/config`

### For Snowflake Deployment
- Snowflake account with Streamlit support
- Access to the `sun_valley` database and `y2025` schema

## Running the App

### Local Development

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Ensure Snowflake connection is configured**:
   - Make sure your `~/.snowsql/config` file contains connection details
   - Verify you have access to the `sun_valley` database and `y2025` schema

3. **Run the app**:
   ```bash
   streamlit run sun_valley_app.py
   ```

4. **Access the app**:
   - Open your browser to `http://localhost:8501`

### Snowflake Deployment

1. **Upload the app** to your Snowflake account
2. **Create a Streamlit app** in Snowflake using the `sun_valley_app.py` file
3. **Ensure permissions** to access the `sun_valley` database and `y2025` schema
4. **Run the app** from within Snowflake

## App Structure

The app consists of several main sections:

### 1. Connection & Context Setup
- Establishes connection to Snowflake
- Sets database context to `sun_valley.y2025`
- Displays connection information

### 2. Table Overview
- Lists all tables in the `y2025` schema
- Shows table statistics (count, rows, size)
- Displays table metadata

### 3. Table Explorer
Interactive exploration with three tabs:
- **📊 Data Preview**: Sample rows from selected table
- **🏗️ Column Info**: Schema and column details
- **📋 Table Details**: Metadata like creation date, size, comments

## Connection Logic

The app uses a smart connection strategy:

1. **Snowflake Environment**: Uses `st.connection('snowflake')` when running in Snowflake
2. **Local Environment**: Uses the `snowflake_connection.py` module with JWT authentication
3. **Fallback**: Multiple connection methods for maximum compatibility

## Error Handling

The app includes comprehensive error handling:
- Connection failures with detailed error messages
- Database/schema access issues
- Table and data query errors
- Graceful degradation when features aren't available

## Customization

To modify the default database and schema, update these constants in `sun_valley_app.py`:

```python
DEFAULT_DATABASE = "sun_valley"
DEFAULT_SCHEMA = "y2025"
```

## Troubleshooting

### Common Issues

1. **Connection Errors**:
   - Verify your Snowflake credentials
   - Check network connectivity
   - Ensure JWT authentication is properly configured

2. **Database Access Errors**:
   - Confirm the `sun_valley` database exists
   - Verify you have access to the `y2025` schema
   - Check your user permissions

3. **Table Not Found**:
   - Ensure tables exist in the `y2025` schema
   - Verify table names are correct
   - Check case sensitivity

### Getting Help

If you encounter issues:
1. Check the Streamlit error messages in the app
2. Review the connection configuration
3. Verify database and schema permissions
4. Check the Snowflake logs for detailed error information

## Development Notes

The app reuses connection logic from the existing `streamlit_app.py` but focuses specifically on the Sun Valley database context. It's designed to be:

- **Portable**: Works in both local and Snowflake environments
- **Robust**: Handles various connection scenarios
- **User-friendly**: Provides clear feedback and error messages
- **Extensible**: Easy to add new features and functionality 