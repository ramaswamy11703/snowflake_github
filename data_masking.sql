-- ============================================================================
-- Data Masking for Roles - Snowflake Analysis
-- ============================================================================
-- This file contains SQL statements for exploring and implementing data masking
-- policies for different roles in Snowflake
-- ============================================================================

-- TASK_001: Create temp database
-- Description: Create TEMP database if it doesn't exist for our experiments
-- Label: create_temp_database
CREATE DATABASE IF NOT EXISTS TEMP;

-- ============================================================================

-- TASK_002: Create schema for masking experiments
-- Description: Create a temporary schema for our data masking experiments  
-- Label: create_schema
CREATE SCHEMA IF NOT EXISTS TEMP.sridhar_masking;

-- ============================================================================

-- TASK_003: Create customer table with PII data
-- Description: Create customer table with sensitive data that needs masking
-- Label: create_customer_table
CREATE OR REPLACE TABLE TEMP.sridhar_masking.customer (
    customerid INTEGER,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    customer_address VARCHAR(200),
    social_security_number VARCHAR(11)
);

-- ============================================================================

-- TASK_004: Insert synthetic customer data
-- Description: Insert sample customer records with realistic PII data
-- Label: insert_customer_data
INSERT INTO TEMP.sridhar_masking.customer VALUES
    (1001, 'John', 'Smith', '123 Main St, Anytown, NY 12345', '123-45-6789'),
    (1002, 'Jane', 'Johnson', '456 Oak Ave, Springfield, IL 62701', '234-56-7890'),
    (1003, 'Michael', 'Williams', '789 Pine Rd, Portland, OR 97201', '345-67-8901'),
    (1004, 'Sarah', 'Brown', '321 Elm St, Austin, TX 78701', '456-78-9012'),
    (1005, 'David', 'Jones', '654 Maple Dr, Denver, CO 80201', '567-89-0123'),
    (1006, 'Lisa', 'Garcia', '987 Cedar Ln, Miami, FL 33101', '678-90-1234'),
    (1007, 'Robert', 'Miller', '147 Birch Ave, Seattle, WA 98101', '789-01-2345'),
    (1008, 'Jennifer', 'Davis', '258 Spruce St, Boston, MA 02101', '890-12-3456'),
    (1009, 'Christopher', 'Rodriguez', '369 Willow Way, Phoenix, AZ 85001', '901-23-4567'),
    (1010, 'Amanda', 'Martinez', '741 Poplar Pl, Atlanta, GA 30301', '012-34-5678'),
    (1011, 'Matthew', 'Anderson', '852 Hickory Hill, Nashville, TN 37201', '123-45-6780'),
    (1012, 'Ashley', 'Taylor', '963 Walnut Walk, San Diego, CA 92101', '234-56-7891'),
    (1013, 'Joshua', 'Thomas', '159 Chestnut Ct, Las Vegas, NV 89101', '345-67-8902'),
    (1014, 'Jessica', 'Hernandez', '357 Sycamore St, Minneapolis, MN 55401', '456-78-9013');

-- ============================================================================

-- TASK_005: Verify table creation and data
-- Description: Check that our customer table was created successfully with all data
-- Label: verify_customer_table
SELECT 
    customerid,
    first_name,
    last_name,
    customer_address,
    social_security_number
FROM TEMP.sridhar_masking.customer
ORDER BY customerid;

-- ============================================================================

-- TASK_006: Create regular role
-- Description: Create regular role for users with limited data access
-- Label: create_regular_role
CREATE ROLE IF NOT EXISTS regular;

-- ============================================================================

-- TASK_007: Create pii_privileged role
-- Description: Create pii_privileged role for users who can see sensitive data
-- Label: create_pii_role
CREATE ROLE IF NOT EXISTS pii_privileged;

-- ============================================================================

-- TASK_008: Grant permissions to regular role
-- Description: Grant necessary permissions for regular role to access customer table
-- Label: grant_regular_permissions
GRANT USAGE ON DATABASE TEMP TO ROLE regular;

-- ============================================================================

-- TASK_009: Grant schema access to regular role
-- Description: Grant schema usage to regular role
-- Label: grant_regular_schema
GRANT USAGE ON SCHEMA TEMP.sridhar_masking TO ROLE regular;

-- ============================================================================

-- TASK_010: Grant table access to regular role
-- Description: Grant SELECT permission on customer table to regular role
-- Label: grant_regular_table
GRANT SELECT ON TABLE TEMP.sridhar_masking.customer TO ROLE regular;

-- ============================================================================

-- TASK_011: Grant permissions to pii_privileged role
-- Description: Grant necessary permissions for pii_privileged role to access customer table
-- Label: grant_pii_permissions
GRANT USAGE ON DATABASE TEMP TO ROLE pii_privileged;

-- ============================================================================

-- TASK_012: Grant schema access to pii_privileged role
-- Description: Grant schema usage to pii_privileged role
-- Label: grant_pii_schema
GRANT USAGE ON SCHEMA TEMP.sridhar_masking TO ROLE pii_privileged;

-- ============================================================================

-- TASK_013: Grant table access to pii_privileged role
-- Description: Grant SELECT permission on customer table to pii_privileged role
-- Label: grant_pii_table
GRANT SELECT ON TABLE TEMP.sridhar_masking.customer TO ROLE pii_privileged;

-- ============================================================================

-- TASK_014: Create masking policy for social security numbers
-- Description: Create a masking policy that shows xxx-xx-xxxx for regular users
-- Label: create_ssn_masking_policy
CREATE OR REPLACE MASKING POLICY TEMP.sridhar_masking.ssn_mask AS (val string) RETURNS string ->
  CASE
    WHEN CURRENT_ROLE() IN ('PII_PRIVILEGED', 'ACCOUNTADMIN', 'SYSADMIN') THEN val
    ELSE 'xxx-xx-xxxx'
  END;

-- ============================================================================

-- TASK_015: Apply masking policy to social security number column
-- Description: Apply the SSN masking policy to the social_security_number column
-- Label: apply_masking_policy
ALTER TABLE TEMP.sridhar_masking.customer 
MODIFY COLUMN social_security_number 
SET MASKING POLICY TEMP.sridhar_masking.ssn_mask;

-- ============================================================================

-- TASK_016: Test masking as ACCOUNTADMIN (should see real data)
-- Description: Query customer data as ACCOUNTADMIN to verify real data is visible
-- Label: test_admin_view
SELECT 
    customerid,
    first_name,
    last_name,
    social_security_number,
    'ACCOUNTADMIN' as current_role_context
FROM TEMP.sridhar_masking.customer
ORDER BY customerid
LIMIT 5;

-- ============================================================================

-- TASK_017: Grant roles to current user for testing
-- Description: Grant the new roles to current user so we can test role switching
-- Label: grant_roles_to_user
GRANT ROLE regular TO USER SRIDHAR2AT2RAMASWAMY2ORG;

-- ============================================================================

-- TASK_018: Grant pii_privileged role to current user
-- Description: Grant pii_privileged role to current user for testing
-- Label: grant_pii_role_to_user
GRANT ROLE pii_privileged TO USER SRIDHAR2AT2RAMASWAMY2ORG;

-- ============================================================================

-- TASK_019: Test data visibility with regular role (should see masked SSNs)
-- Description: Switch to regular role and query data to see masking in action
-- Label: test_regular_role_masking
USE ROLE regular;

-- ============================================================================

-- TASK_020: Query as regular role (SSNs should be masked)
-- Description: Query customer data as regular role - SSNs should show xxx-xx-xxxx
-- Label: query_as_regular_role
SELECT 
    customerid,
    first_name,
    last_name,
    social_security_number,
    'REGULAR' as current_role_context
FROM TEMP.sridhar_masking.customer
ORDER BY customerid
LIMIT 5;

-- ============================================================================

-- TASK_021: Switch to pii_privileged role
-- Description: Switch to pii_privileged role to see unmasked data
-- Label: switch_to_pii_role
USE ROLE pii_privileged;

-- ============================================================================

-- TASK_022: Query as pii_privileged role (should see real SSNs)
-- Description: Query customer data as pii_privileged role - should see real SSNs
-- Label: query_as_pii_role
SELECT 
    customerid,
    first_name,
    last_name,
    social_security_number,
    'PII_PRIVILEGED' as current_role_context
FROM TEMP.sridhar_masking.customer
ORDER BY customerid
LIMIT 5;

-- ============================================================================

-- TASK_023: Switch back to ACCOUNTADMIN
-- Description: Switch back to ACCOUNTADMIN role
-- Label: switch_back_to_admin
USE ROLE ACCOUNTADMIN;

-- ============================================================================
-- End of Data Masking Analysis File
-- ============================================================================ 