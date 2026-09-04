-- DDL for DEPOC-44: Automated Customer Data Ingestion Pipeline
-- Co-authored with CoCo

USE DATABASE DE_POC;
USE SCHEMA PUBLIC;
USE WAREHOUSE COMPUTE_WH;

-- =============================================================
-- 1. File Format for CSV ingestion
-- =============================================================
CREATE FILE FORMAT IF NOT EXISTS DE_POC.PUBLIC.CUSTOMER_CSV_FORMAT
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('', 'NULL', 'null')
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- =============================================================
-- 2. Internal Stage for customer data files
-- =============================================================
CREATE STAGE IF NOT EXISTS DE_POC.PUBLIC.CUSTOMER_DATA_STAGE
    FILE_FORMAT = DE_POC.PUBLIC.CUSTOMER_CSV_FORMAT;

-- =============================================================
-- 3. Raw Staging Table (landing zone for COPY INTO)
-- =============================================================
CREATE TABLE IF NOT EXISTS DE_POC.PUBLIC.RAW_CUSTOMER_STAGING (
    CUSTOMER_ID    VARCHAR(50),
    FIRST_NAME     VARCHAR(100),
    LAST_NAME      VARCHAR(100),
    EMAIL          VARCHAR(255),
    PHONE          VARCHAR(50),
    ADDRESS        VARCHAR(500),
    CITY           VARCHAR(100),
    STATE          VARCHAR(50),
    ZIP_CODE       VARCHAR(20),
    COUNTRY        VARCHAR(100),
    CREATED_DATE   VARCHAR(30),
    UPDATED_DATE   VARCHAR(30),
    _SOURCE_FILE   VARCHAR(500),
    _LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

ALTER TABLE DE_POC.PUBLIC.RAW_CUSTOMER_STAGING SET CHANGE_TRACKING = TRUE;

-- =============================================================
-- 4. Stream on staging for CDC / incremental detection
-- =============================================================
CREATE STREAM IF NOT EXISTS DE_POC.PUBLIC.RAW_CUSTOMER_STREAM
    ON TABLE DE_POC.PUBLIC.RAW_CUSTOMER_STAGING
    APPEND_ONLY = TRUE;

-- =============================================================
-- 5. Target Customer Table
-- =============================================================
CREATE TABLE IF NOT EXISTS DE_POC.PUBLIC.CUSTOMER (
    CUSTOMER_ID   VARCHAR(50)  NOT NULL PRIMARY KEY,
    FIRST_NAME    VARCHAR(100),
    LAST_NAME     VARCHAR(100),
    EMAIL         VARCHAR(255),
    PHONE         VARCHAR(50),
    ADDRESS       VARCHAR(500),
    CITY          VARCHAR(100),
    STATE         VARCHAR(50),
    ZIP_CODE      VARCHAR(20),
    COUNTRY       VARCHAR(100),
    CREATED_DATE  TIMESTAMP_NTZ,
    UPDATED_DATE  TIMESTAMP_NTZ,
    _LOAD_ID      VARCHAR(36),
    _SOURCE_FILE  VARCHAR(500),
    _LOADED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _UPDATED_AT   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =============================================================
-- 6. Error Log Table
-- =============================================================
CREATE TABLE IF NOT EXISTS DE_POC.PUBLIC.CUSTOMER_ERROR_LOG (
    ERROR_ID        NUMBER AUTOINCREMENT PRIMARY KEY,
    LOAD_ID         VARCHAR(36),
    SOURCE_FILE     VARCHAR(500),
    CUSTOMER_ID     VARCHAR(50),
    RECORD_CONTENT  VARIANT,
    ERROR_REASON    VARCHAR(1000),
    ERROR_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =============================================================
-- 7. Load Tracking Table
-- =============================================================
CREATE TABLE IF NOT EXISTS DE_POC.PUBLIC.CUSTOMER_LOAD_TRACKING (
    LOAD_ID            VARCHAR(36) NOT NULL PRIMARY KEY,
    SOURCE_FILE        VARCHAR(500),
    LOAD_TIMESTAMP     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    RECORDS_STAGED     NUMBER DEFAULT 0,
    RECORDS_VALID      NUMBER DEFAULT 0,
    RECORDS_REJECTED   NUMBER DEFAULT 0,
    RECORDS_INSERTED   NUMBER DEFAULT 0,
    RECORDS_UPDATED    NUMBER DEFAULT 0,
    RECORDS_DUPLICATE  NUMBER DEFAULT 0,
    PROCESSING_STATUS  VARCHAR(20) DEFAULT 'PENDING',
    ERROR_MESSAGE      VARCHAR(2000),
    COMPLETED_AT       TIMESTAMP_NTZ
);