#!/usr/bin/env python3
"""
ClickHouse Connection Module

This module provides common functionality for connecting to ClickHouse databases.
"""

import os
import sys
import clickhouse_connect


def read_clickhouse_password(password_file="~/.snowsql/clickhouse"):
    """
    Read the ClickHouse API user password from the specified file
    
    Args:
        password_file: Path to the file containing the API user password
    
    Returns:
        str: The API user password, or None if file not found
    """
    try:
        password_path = os.path.expanduser(password_file)
        
        if not os.path.exists(password_path):
            print(f"❌ Password file not found at {password_path}")
            return None
        
        with open(password_path, 'r') as f:
            password = f.read().strip()
        
        print(f"✅ API user password loaded from {password_path}")
        return password
        
    except Exception as e:
        print(f"❌ Error reading password file: {e}")
        return None


def connect_to_clickhouse(api_user="default", api_user_password=None, password_file="~/.snowsql/clickhouse"):
    """
    Connect to ClickHouse Cloud using the specific host
    
    Args:
        api_user: API user for authentication
        api_user_password: API user password (if None, will read from file)
        password_file: Path to password file if api_user_password is None
    
    Returns:
        ClickHouse client object or None if failed
    """
    host = "kzb8n9b9bc.us-west-2.aws.clickhouse.cloud"
    port = 8443
    
    # Read password from file if not provided
    if api_user_password is None:
        api_user_password = read_clickhouse_password(password_file)
        if not api_user_password:
            return None
    
    print(f"🔗 Connecting to ClickHouse Cloud...")
    print(f"   Host: {host}")
    print(f"   Port: {port}")
    print(f"   API User: {api_user}")
    
    try:
        client = clickhouse_connect.get_client(
            host=host,
            username=api_user,
            password=api_user_password,
            port=port,
            secure=True
        )
        
        # Test the connection
        result = client.query("SELECT version()")
        version = result.result_rows[0][0]
        print(f"✅ Successfully connected to ClickHouse Cloud!")
        print(f"   ClickHouse Version: {version}")
        
        return client
        
    except Exception as e:
        print(f"❌ Error connecting to ClickHouse: {e}")
        return None


def test_connection(client):
    """
    Test the ClickHouse connection
    
    Args:
        client: ClickHouse client object
    
    Returns:
        bool: True if connection is working, False otherwise
    """
    if client is None:
        return False
    
    try:
        result = client.query("SELECT 1")
        return True
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return False 