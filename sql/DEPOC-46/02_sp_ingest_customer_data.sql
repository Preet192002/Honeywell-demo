-- DEPOC-46: Main pipeline stored procedure for customer data ingestion
-- Co-authored with CoCo

CREATE OR REPLACE PROCEDURE DE_POC.PUBLIC.SP_INGEST_CUSTOMER_DATA(
    P_SOURCE_FILE VARCHAR DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_load_id       VARCHAR;
    v_source_file   VARCHAR;
    v_total_rows    NUMBER DEFAULT 0;
    v_valid_rows    NUMBER DEFAULT 0;
    v_invalid_rows  NUMBER DEFAULT 0;
    v_inserted_rows NUMBER DEFAULT 0;
    v_updated_rows  NUMBER DEFAULT 0;
    v_duplicate_rows NUMBER DEFAULT 0;
BEGIN
    v_load_id := 'LOAD_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS') || '_' || SUBSTR(UUID_STRING(), 1, 8);
    v_source_file := COALESCE(:P_SOURCE_FILE, 'CUSTOMER_DATA_STAGE');

    -- Record load start
    INSERT INTO DE_POC.PUBLIC.PIPELINE_LOAD_TRACKING (LOAD_ID, SOURCE_FILE, LOAD_TIMESTAMP, STATUS)
    VALUES (:v_load_id, :v_source_file, CURRENT_TIMESTAMP(), 'RUNNING');

    -- Truncate staging and load from stage
    TRUNCATE TABLE DE_POC.PUBLIC.RAW_CUSTOMERS;
    COPY INTO DE_POC.PUBLIC.RAW_CUSTOMERS (
        CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE,
        ADDRESS, CITY, STATE, COUNTRY, POSTAL_CODE,
        CREATED_DATE, UPDATED_DATE
    )
    FROM @DE_POC.PUBLIC.CUSTOMER_DATA_STAGE
    FILE_FORMAT = DE_POC.PUBLIC.CSV_CUSTOMER_FORMAT
    PATTERN = '.*\.csv'
    ON_ERROR = 'CONTINUE';

    -- Tag loaded rows
    UPDATE DE_POC.PUBLIC.RAW_CUSTOMERS
    SET _LOAD_ID = :v_load_id, _LOAD_FILE = :v_source_file, _LOAD_TIMESTAMP = CURRENT_TIMESTAMP()
    WHERE _LOAD_ID IS NULL;

    SELECT COUNT(*) INTO :v_total_rows FROM DE_POC.PUBLIC.RAW_CUSTOMERS WHERE _LOAD_ID = :v_load_id;

    -- Idempotency check
    LET v_already_processed NUMBER := 0;
    SELECT COUNT(*) INTO :v_already_processed
    FROM DE_POC.PUBLIC.PIPELINE_LOAD_TRACKING
    WHERE SOURCE_FILE = :v_source_file AND STATUS = 'SUCCESS' AND LOAD_ID != :v_load_id;

    IF (:v_already_processed > 0 AND :v_total_rows = 0) THEN
        UPDATE DE_POC.PUBLIC.PIPELINE_LOAD_TRACKING
        SET STATUS = 'SKIPPED', ERROR_MESSAGE = 'Source already processed. No new files.', TOTAL_ROWS = 0, COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE LOAD_ID = :v_load_id;
        RETURN OBJECT_CONSTRUCT('load_id', :v_load_id, 'status', 'SKIPPED');
    END IF;

    -- Validation: missing CUSTOMER_ID
    INSERT INTO DE_POC.PUBLIC.PIPELINE_ERROR_LOG (LOAD_ID, SOURCE_FILE, RECORD_DATA, ERROR_TYPE, ERROR_DETAILS, CUSTOMER_ID)
    SELECT :v_load_id, :v_source_file,
           OBJECT_CONSTRUCT('CUSTOMER_ID', CUSTOMER_ID, 'FIRST_NAME', FIRST_NAME, 'LAST_NAME', LAST_NAME, 'EMAIL', EMAIL),
           'MISSING_CUSTOMER_ID', 'Customer ID is NULL or empty', CUSTOMER_ID
    FROM DE_POC.PUBLIC.RAW_CUSTOMERS
    WHERE _LOAD_ID = :v_load_id AND (CUSTOMER_ID IS NULL OR TRIM(CUSTOMER_ID) = '');

    -- Validation: invalid email
    INSERT INTO DE_POC.PUBLIC.PIPELINE_ERROR_LOG (LOAD_ID, SOURCE_FILE, RECORD_DATA, ERROR_TYPE, ERROR_DETAILS, CUSTOMER_ID)
    SELECT :v_load_id, :v_source_file,
           OBJECT_CONSTRUCT('CUSTOMER_ID', CUSTOMER_ID, 'EMAIL', EMAIL),
           'INVALID_EMAIL', 'Email format is invalid', CUSTOMER_ID
    FROM DE_POC.PUBLIC.RAW_CUSTOMERS
    WHERE _LOAD_ID = :v_load_id AND CUSTOMER_ID IS NOT NULL AND TRIM(CUSTOMER_ID) != ''
      AND EMAIL IS NOT NULL AND NOT RLIKE(EMAIL, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$');

    -- Validation: intra-batch duplicates
    INSERT INTO DE_POC.PUBLIC.PIPELINE_ERROR_LOG (LOAD_ID, SOURCE_FILE, RECORD_DATA, ERROR_TYPE, ERROR_DETAILS, CUSTOMER_ID)
    SELECT :v_load_id, :v_source_file,
           OBJECT_CONSTRUCT('CUSTOMER_ID', r.CUSTOMER_ID, 'FIRST_NAME', r.FIRST_NAME),
           'INTRA_BATCH_DUPLICATE', 'Duplicate CUSTOMER_ID within batch', r.CUSTOMER_ID
    FROM DE_POC.PUBLIC.RAW_CUSTOMERS r
    INNER JOIN (
        SELECT CUSTOMER_ID FROM DE_POC.PUBLIC.RAW_CUSTOMERS
        WHERE _LOAD_ID = :v_load_id AND CUSTOMER_ID IS NOT NULL AND TRIM(CUSTOMER_ID) != ''
        GROUP BY CUSTOMER_ID HAVING COUNT(*) > 1
    ) d ON r.CUSTOMER_ID = d.CUSTOMER_ID
    WHERE r._LOAD_ID = :v_load_id;

    -- Count invalids and duplicates
    SELECT COUNT(DISTINCT CUSTOMER_ID) INTO :v_invalid_rows
    FROM DE_POC.PUBLIC.PIPELINE_ERROR_LOG
    WHERE LOAD_ID = :v_load_id AND ERROR_TYPE IN ('MISSING_CUSTOMER_ID', 'INVALID_EMAIL');

    v_valid_rows := :v_total_rows - :v_invalid_rows;

    SELECT COUNT(*) INTO :v_duplicate_rows
    FROM DE_POC.PUBLIC.PIPELINE_ERROR_LOG
    WHERE LOAD_ID = :v_load_id AND ERROR_TYPE = 'INTRA_BATCH_DUPLICATE';

    -- MERGE: incremental upsert with deduplication
    MERGE INTO DE_POC.PUBLIC.CUSTOMERS tgt
    USING (
        SELECT * FROM (
            SELECT CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE,
                ADDRESS, CITY, STATE, COUNTRY, POSTAL_CODE,
                TRY_TO_TIMESTAMP_NTZ(CREATED_DATE) AS CREATED_DATE,
                TRY_TO_TIMESTAMP_NTZ(UPDATED_DATE) AS UPDATED_DATE,
                _LOAD_ID,
                ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY UPDATED_DATE DESC NULLS LAST) AS rn
            FROM DE_POC.PUBLIC.RAW_CUSTOMERS
            WHERE _LOAD_ID = :v_load_id
              AND CUSTOMER_ID IS NOT NULL AND TRIM(CUSTOMER_ID) != ''
              AND CUSTOMER_ID NOT IN (
                  SELECT DISTINCT CUSTOMER_ID FROM DE_POC.PUBLIC.PIPELINE_ERROR_LOG
                  WHERE LOAD_ID = :v_load_id AND ERROR_TYPE = 'INVALID_EMAIL' AND CUSTOMER_ID IS NOT NULL
              )
        ) WHERE rn = 1
    ) src
    ON tgt.CUSTOMER_ID = src.CUSTOMER_ID
    WHEN MATCHED AND (src.UPDATED_DATE > tgt.UPDATED_DATE OR tgt.UPDATED_DATE IS NULL) THEN UPDATE SET
        tgt.FIRST_NAME = src.FIRST_NAME, tgt.LAST_NAME = src.LAST_NAME,
        tgt.EMAIL = src.EMAIL, tgt.PHONE = src.PHONE,
        tgt.ADDRESS = src.ADDRESS, tgt.CITY = src.CITY,
        tgt.STATE = src.STATE, tgt.COUNTRY = src.COUNTRY,
        tgt.POSTAL_CODE = src.POSTAL_CODE,
        tgt.CREATED_DATE = src.CREATED_DATE, tgt.UPDATED_DATE = src.UPDATED_DATE,
        tgt._UPDATED_AT = CURRENT_TIMESTAMP(), tgt._LOAD_ID = src._LOAD_ID, tgt._IS_ACTIVE = TRUE
    WHEN NOT MATCHED THEN INSERT (
        CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE, ADDRESS, CITY, STATE, COUNTRY, POSTAL_CODE,
        CREATED_DATE, UPDATED_DATE, _INSERTED_AT, _UPDATED_AT, _LOAD_ID, _IS_ACTIVE
    ) VALUES (
        src.CUSTOMER_ID, src.FIRST_NAME, src.LAST_NAME, src.EMAIL, src.PHONE,
        src.ADDRESS, src.CITY, src.STATE, src.COUNTRY, src.POSTAL_CODE,
        src.CREATED_DATE, src.UPDATED_DATE,
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src._LOAD_ID, TRUE
    );

    -- Capture results
    SELECT COUNT(*) INTO :v_inserted_rows FROM DE_POC.PUBLIC.CUSTOMERS
    WHERE _LOAD_ID = :v_load_id AND _INSERTED_AT = _UPDATED_AT;
    SELECT COUNT(*) INTO :v_updated_rows FROM DE_POC.PUBLIC.CUSTOMERS
    WHERE _LOAD_ID = :v_load_id AND _INSERTED_AT != _UPDATED_AT;

    -- Update tracking
    UPDATE DE_POC.PUBLIC.PIPELINE_LOAD_TRACKING
    SET TOTAL_ROWS = :v_total_rows, VALID_ROWS = :v_valid_rows, INVALID_ROWS = :v_invalid_rows,
        INSERTED_ROWS = :v_inserted_rows, UPDATED_ROWS = :v_updated_rows, DUPLICATE_ROWS = :v_duplicate_rows,
        STATUS = 'SUCCESS', COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE LOAD_ID = :v_load_id;

    RETURN OBJECT_CONSTRUCT(
        'load_id', :v_load_id, 'source_file', :v_source_file, 'status', 'SUCCESS',
        'total_rows', :v_total_rows, 'valid_rows', :v_valid_rows, 'invalid_rows', :v_invalid_rows,
        'inserted_rows', :v_inserted_rows, 'updated_rows', :v_updated_rows, 'duplicate_rows', :v_duplicate_rows
    );
EXCEPTION
    WHEN OTHER THEN
        LET v_err VARCHAR := SQLERRM;
        UPDATE DE_POC.PUBLIC.PIPELINE_LOAD_TRACKING
        SET STATUS = 'FAILED', ERROR_MESSAGE = :v_err, COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE LOAD_ID = :v_load_id;
        RETURN OBJECT_CONSTRUCT('load_id', :v_load_id, 'status', 'FAILED', 'error', :v_err);
END;
$$;