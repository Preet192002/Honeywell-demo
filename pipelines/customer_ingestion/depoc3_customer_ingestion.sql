-- DEPOC-3: Customer CSV ingestion pipeline implementation
-- Co-authored with CoCo
--
-- Objects created:
--   FILE FORMAT: DE_POC.PUBLIC.CUSTOMER_CSV_FORMAT
--   STAGE:       DE_POC.PUBLIC.CUSTOMER_CSV_STAGE
--   TABLE:       DE_POC.PUBLIC.STG_CUSTOMERS (staging)
--   TABLE:       DE_POC.PUBLIC.CUSTOMERS (target)
--   TABLE:       DE_POC.PUBLIC.CUSTOMER_INGESTION_LOG (error/audit log)
--   PROCEDURE:   DE_POC.PUBLIC.SP_INGEST_CUSTOMERS(P_SOURCE_FILE VARCHAR)
--
-- Usage:
--   1. PUT file into @CUSTOMER_CSV_STAGE
--   2. CALL SP_INGEST_CUSTOMERS();  -- processes all files in stage
--   or CALL SP_INGEST_CUSTOMERS('customers.csv');  -- specific file

-- ============================================================
-- FILE FORMAT
-- ============================================================
CREATE OR REPLACE FILE FORMAT DE_POC.PUBLIC.CUSTOMER_CSV_FORMAT
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('', 'NULL', 'null')
  TRIM_SPACE = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- ============================================================
-- INTERNAL STAGE
-- ============================================================
CREATE OR REPLACE STAGE DE_POC.PUBLIC.CUSTOMER_CSV_STAGE
  FILE_FORMAT = DE_POC.PUBLIC.CUSTOMER_CSV_FORMAT
  COMMENT = 'DEPOC-3: Landing stage for customer CSV ingestion';

-- ============================================================
-- STAGING TABLE (raw load)
-- ============================================================
CREATE OR REPLACE TABLE DE_POC.PUBLIC.STG_CUSTOMERS (
  CUSTOMER_ID   VARCHAR(50),
  FIRST_NAME    VARCHAR(100),
  LAST_NAME     VARCHAR(100),
  EMAIL         VARCHAR(255),
  PHONE         VARCHAR(50),
  ADDRESS       VARCHAR(500),
  CITY          VARCHAR(100),
  STATE         VARCHAR(100),
  COUNTRY       VARCHAR(100),
  CREATED_DATE  VARCHAR(50),
  _LOADED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  _SOURCE_FILE  VARCHAR(500)
)
COMMENT = 'DEPOC-3: Staging table for raw customer CSV data';

-- ============================================================
-- TARGET TABLE (validated records)
-- ============================================================
CREATE OR REPLACE TABLE DE_POC.PUBLIC.CUSTOMERS (
  CUSTOMER_ID   VARCHAR(50)   NOT NULL,
  FIRST_NAME    VARCHAR(100)  NOT NULL,
  LAST_NAME     VARCHAR(100)  NOT NULL,
  EMAIL         VARCHAR(255)  NOT NULL,
  PHONE         VARCHAR(50),
  ADDRESS       VARCHAR(500),
  CITY          VARCHAR(100),
  STATE         VARCHAR(100),
  COUNTRY       VARCHAR(100),
  CREATED_DATE  DATE,
  _LOADED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  _SOURCE_FILE  VARCHAR(500),
  CONSTRAINT PK_CUSTOMERS PRIMARY KEY (CUSTOMER_ID)
)
COMMENT = 'DEPOC-3: Target table for validated customer records';

-- ============================================================
-- ERROR / AUDIT LOG TABLE
-- ============================================================
CREATE OR REPLACE TABLE DE_POC.PUBLIC.CUSTOMER_INGESTION_LOG (
  LOG_ID          NUMBER AUTOINCREMENT,
  LOG_TIMESTAMP   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  SEVERITY        VARCHAR(10),
  COMPONENT       VARCHAR(100) DEFAULT 'CUSTOMER_INGESTION',
  ISSUE_KEY       VARCHAR(20) DEFAULT 'DEPOC-3',
  RECORD_DATA     VARIANT,
  ERROR_REASON    VARCHAR(1000),
  SOURCE_FILE     VARCHAR(500),
  BATCH_ID        VARCHAR(50)
)
COMMENT = 'DEPOC-3: Error and audit log for customer ingestion pipeline';
