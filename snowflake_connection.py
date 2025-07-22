#!/usr/bin/env python3
"""
Snowflake Database Connection Module

This module provides common functionality for connecting to Snowflake databases
using configuration from the SnowSQL config file located at ~/.snowsql/config
"""

import configparser
import os
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.backends import default_backend


def load_private_key(private_key_path):
    """
    Load the private key from the specified path for JWT authentication
    """
    try:
        with open(private_key_path, 'rb') as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None,  # Assuming no password on the private key
                backend=default_backend()
            )
        
        # Convert to DER format for Snowflake connector
        private_key_der = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        
        return private_key_der
    except Exception as e:
        print(f"Error loading private key: {e}")
        return None


def read_snowsql_config(config_path="~/.snowsql/config"):
    """
    Read the SnowSQL configuration file and return connection parameters
    """
    config_path = os.path.expanduser(config_path)
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at {config_path}")
    
    config = configparser.ConfigParser()
    config.read(config_path)
    
    return config


def connect_to_snowflake(connection_name="my_conn", config_path="~/.snowsql/config"):
    """
    Connect to Snowflake using parameters from SnowSQL config file
    
    Args:
        connection_name (str): Name of the connection in the config file
        config_path (str): Path to the SnowSQL config file
    
    Returns:
        snowflake.connector.connection.SnowflakeConnection: Database connection object
    """
    try:
        # Read configuration
        config = read_snowsql_config(config_path)
        
        # Try to get connection parameters from the specified connection
        section_name = f"connections.{connection_name}"
        if section_name not in config:
            # Fall back to account name section if available
            available_sections = [s for s in config.sections() if not s.startswith('connections.')]
            if available_sections:
                section_name = available_sections[0]
                print(f"Connection '{connection_name}' not found, using '{section_name}'")
            else:
                raise ValueError(f"No valid connection configuration found")
        
        section = config[section_name]
        
        # Extract connection parameters
        account = section.get('accountname', section.get('account'))
        username = section.get('username', section.get('user'))
        private_key_path = section.get('private_key_path')
        password = section.get('password')

        # Remove quotes from paths if present
        if private_key_path:
            private_key_path = private_key_path.strip('"\'')
        if account:
            account = account.strip('"\'')
        if username:
            username = username.strip('"\'')
        if password:
            password = password.strip('"\'')

        if not account or not username:
            raise ValueError("Missing required connection parameters (account, username)")

        if not private_key_path and not password:
            raise ValueError("Missing authentication method (private_key_path or password)")

        print(f"Connecting to Snowflake...")
        print(f"Account: {account}")
        print(f"Username: {username}")

        # Create connection based on available authentication method
        if private_key_path:
            # Use JWT authentication with private key
            private_key_der = load_private_key(os.path.expanduser(private_key_path))
            if private_key_der is None:
                raise ValueError("Failed to load private key")

            print(f"Using JWT authentication with private key")
            conn = snowflake.connector.connect(
                account=account,
                user=username,
                private_key=private_key_der,
                authenticator='SNOWFLAKE_JWT'
            )
        else:
            # Use password authentication
            print(f"Using password authentication")
            conn = snowflake.connector.connect(
                account=account,
                user=username,
                password=password
            )
        
        print("✅ Successfully connected to Snowflake!")
        return conn
        
    except Exception as e:
        print(f"❌ Error connecting to Snowflake: {e}")
        return None


def test_connection(conn):
    """
    Test the connection by running a simple query
    """
    if conn is None:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION(), CURRENT_USER(), CURRENT_ACCOUNT()")
        result = cursor.fetchone()
        
        print("\n=== Connection Test Results ===")
        print(f"Snowflake Version: {result[0]}")
        print(f"Current User: {result[1]}")
        print(f"Current Account: {result[2]}")
        
        cursor.close()
        return True
        
    except Exception as e:
        print(f"❌ Error testing connection: {e}")
        return False


def get_connection_params(config_path="~/.snowsql/config", connection_name="my_conn"):
    """
    Extract connection parameters for use in threading
    """
    try:
        config = read_snowsql_config(config_path)
        
        # Try to get connection parameters from the specified connection
        section_name = f"connections.{connection_name}"
        if section_name not in config:
            # Fall back to account name section if available
            available_sections = [s for s in config.sections() if not s.startswith('connections.')]
            if available_sections:
                section_name = available_sections[0]
        
        section = config[section_name]
        
        # Extract connection parameters
        account = section.get('accountname', section.get('account'))
        username = section.get('username', section.get('user'))
        private_key_path = section.get('private_key_path')
        password = section.get('password')

        # Remove quotes from paths if present
        if private_key_path:
            private_key_path = private_key_path.strip('"\'')
        if account:
            account = account.strip('"\'')
        if username:
            username = username.strip('"\'')
        if password:
            password = password.strip('"\'')

        params = {
            'account': account,
            'user': username
        }

        # Add authentication method
        if private_key_path:
            private_key_der = load_private_key(os.path.expanduser(private_key_path))
            if private_key_der is None:
                raise ValueError("Failed to load private key")
            params['private_key'] = private_key_der
        elif password:
            params['password'] = password
        else:
            raise ValueError("Missing authentication method (private_key_path or password)")

        return params
        
    except Exception as e:
        print(f"❌ Error getting connection parameters: {e}")
        return None 