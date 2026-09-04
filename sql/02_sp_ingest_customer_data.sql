-- Stored Procedure: Incremental customer data ingestion with validation, dedup, and error logging
-- Co-authored with CoCo

CREATE OR REPLACE PROCEDURE DE_POC.PUBLIC.SP_INGEST_CUSTOMER_DATA()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_load_id        VARCHAR DEFAULT UUID_STRING();
    v_source_file    VARCHAR DEFAULT '';
    v_staged_count   NUMBER DEFAULT 0;
    v_valid_count    NUMBER DEFAULT 0;
    v_rejected_count NUMBER DEFAULT 0;
    v_inserted_count NUMBER DEFAULT 0;
    v_updated_count  NUMBER DEFAULT 0;
    v_dup_count      NUMBER DEFAULT 0;
BEGIN
    -- Initialize load tracking record
    INSERT INTO DE_POC.PUBLIC.CUSTOMER_LOAD_TRACKING (LOAD_ID, SOURCE_FILE, RECORDS_STAGED, PROCESSING_STATUS)
    VALUES (:v_load_id, 'pending', 0, 'PROCESSING');

    -- Stage new files from internal stage (idempotent via COPY INTO metadata tracking)
    COPY INTO DE_POC.PUBLIC.RAW_CUSTOMER_STAGING (
        CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE,
        ADDRESS, CITY, STATE, ZIP_CODE, COUNTRY,
        CREATED_DATE, UPDATED_DATE, _SOURCE_FILE
    )
    FROM (
        SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, METADATA$FILENAME
        FROM @DE_POC.PUBLIC.CUSTOMER_DATA_STAGE
    )
    FILE_FORMAT = (FORMAT_NAME = 'DE_POC.PUBLIC.CUSTOMER_CSV_FORMAT')
    ON_ERROR = 'CONTINUE'
    PURGE = FALSE;

    -- Consume stream: capture only new records since last consumption
    CREATE OR REPLACE TEMPORARY TABLE DE_POC.PUBLIC.TEMP_CUSTOMER_BATCH AS
    SELECT * FROM DE_POC.PUBLIC.RAW_CUSTOMER_STREAM;

    SELECT COUNT(*) INTO :v_staged_count FROM DE_POC.PUBLIC.TEMP_CUSTOMER_BATCH;

    SELECT LISTAGG(DISTINCT _SOURCE_FILE, ', ') INTO :v_source_file
    FROM DE_POC.PUBLIC.TEMP_CUSTOMER_BATCH;

    -- Exit early if no new data
    IF (v_staged_count = 0) THEN
        DELETE FROM DE_POC.PUBLIC.CUSTOMER_LOAD_TRACKING WHERE LOAD_ID = :v_load_id;
        RETURN OBJECT_CONSTRUCT('load_id', :v_load_id, 'status', 'NO_NEW_DATA', 'message', 'No new files to process');
    END IF;

    UPDATE DE_POC.PUBLIC.CUSTOMER_LOAD_TRACKING
    SET SOURCE_FILE = :v_source_file, RECORDS_STAGED = :v_staged_count
    WHERE LOAD_ID = :v_load_id;

    -- Validation: reject records with missing required fields or invalid email
    INSERT INTO DE_POC.PUBLIC.CUSTOMER_ERROR_LOG (LOAD_ID, SOURCE_FILE, CUSTOMER_ID, RECORD_CONTENT, ERROR_REASON)
    SELECT
        :v_load_id, _SOURCE_FILE, CUSTOMER_ID,
        OBJECT_CONSTRUCT('customer_id', CUSTOMER_ID, 'first_name', FIRST_NAME, 'last_name', LAST_NAME,
                         'email', EMAIL, 'phone', PHONE, 'address', ADDRESS, 'city', CITY,
                         'state', STATE, 'zip_code', ZIP_CODE, 'country', COUNTRY),
        CASE
            WHEN CUSTOMER_ID IS NULL OR TRIM(CUSTOMER_ID) = '' THEN 'CUSTOMER_ID is null or empty'
            WHEN FIRST_NAME IS NULL OR TRIM(FIRST_NAME) = '' THEN 'FIRST_NAME is null or empty'
            WHEN LAST_NAME IS NULL OR TRIM(LAST_NAME) = '' THEN 'LAST_NAME is null or empty'
            WHEN EMAIL IS NOT NULL AND TRIM(EMAIL) != '' AND EMAIL NOT LIKE '%@%.%' THEN 'EMAIL format invalid'
        END
    FROM DE_POC.PUBLIC.TEMP_CUSTOMER_BATCH
    WHERE CUSTOMER_ID IS NULL OR TRIM(CUSTOMER_ID) = ''
       OR FIRST_NAME IS NULL OR TRIM(FIRST_NAME) = ''
       OR LAST_NAME IS NULL OR TRIM(LAST_NAME) = ''
       OR (EMAIL IS NOT NULL AND TRIM(EMAIL) != '' AND EMAIL NOT LIKE '%@%.%');

    SELECT COUNT(*) INTO :v_rejected_count
    FROM DE_POC.PUBLIC.CUSTOMER_ERROR_LOG WHERE LOAD_ID = :v_load_id;

    v_valid_count := v_staged_count - v_rejected_count;

    -- MERGE: insert new, update changed, skip duplicates
    MERGE INTO DE_POC.PUBLIC.CUSTOMER AS tgt
    USING (
        SELECT * FROM (
            SELECT
                CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE,
                ADDRESS, CITY, STATE, ZIP_CODE, COUNTRY,
                TRY_TO_TIMESTAMP_NTZ(CREATED_DATE) AS CREATED_DATE,
                TRY_TO_TIMESTAMP_NTZ(UPDATED_DATE) AS UPDATED_DATE,
                _SOURCE_FILE,
                ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY UPDATED_DATE DESC, _LOAD_TIMESTAMP DESC) AS rn
            FROM DE_POC.PUBLIC.TEMP_CUSTOMER_BATCH
            WHERE CUSTOMER_ID IS NOT NULL AND TRIM(CUSTOMER_ID) != ''
              AND FIRST_NAME IS NOT NULL AND TRIM(FIRST_NAME) != ''
              AND LAST_NAME IS NOT NULL AND TRIM(LAST_NAME) != ''
              AND (EMAIL IS NULL OR TRIM(EMAIL) = '' OR EMAIL LIKE '%@%.%')
        ) WHERE rn = 1
    ) AS src
    ON tgt.CUSTOMER_ID = src.CUSTOMER_ID
    WHEN MATCHED AND (
        NVL(tgt.FIRST_NAME,'') != NVL(src.FIRST_NAME,'') OR NVL(tgt.LAST_NAME,'') != NVL(src.LAST_NAME,'')
        OR NVL(tgt.EMAIL,'') != NVL(src.EMAIL,'')
        OR NVL(tgt.PHONE,'') != NVL(src.PHONE,'')
        OR NVL(tgt.ADDRESS,'') != NVL(src.ADDRESS,'')
        OR NVL(tgt.CITY,'') != NVL(src.CITY,'')
        OR NVL(tgt.STATE,'') != NVL(src.STATE,'')
        OR NVL(tgt.ZIP_CODE,'') != NVL(src.ZIP_CODE,'')
        OR NVL(tgt.COUNTRY,'') != NVL(src.COUNTRY,'')
    ) THEN UPDATE SET
        tgt.FIRST_NAME = src.FIRST_NAME, tgt.LAST_NAME = src.LAST_NAME,
        tgt.EMAIL = src.EMAIL, tgt.PHONE = src.PHONE,
        tgt.ADDRESS = src.ADDRESS, tgt.CITY = src.CITY,
        tgt.STATE = src.STATE, tgt.ZIP_CODE = src.ZIP_CODE,
        tgt.COUNTRY = src.COUNTRY, tgt.UPDATED_DATE = src.UPDATED_DATE,
        tgt._LOAD_ID = :v_load_id, tgt._SOURCE_FILE = src._SOURCE_FILE,
        tgt._UPDATED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE,
        ADDRESS, CITY, STATE, ZIP_CODE, COUNTRY,
        CREATED_DATE, UPDATED_DATE, _LOAD_ID, _SOURCE_FILE, _LOADED_AT, _UPDATED_AT
    ) VALUES (
        src.CUSTOMER_ID, src.FIRST_NAME, src.LAST_NAME, src.EMAIL, src.PHONE,
        src.ADDRESS, src.CITY, src.STATE, src.ZIP_CODE, src.COUNTRY,
        src.CREATED_DATE, src.UPDATED_DATE,
        :v_load_id, src._SOURCE_FILE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    );

    -- Count inserts vs updates
    SELECT COUNT(*) INTO :v_inserted_count
    FROM DE_POC.PUBLIC.CUSTOMER WHERE _LOAD_ID = :v_load_id AND _LOADED_AT = _UPDATED_AT;

    SELECT COUNT(*) INTO :v_updated_count
    FROM DE_POC.PUBLIC.CUSTOMER WHERE _LOAD_ID = :v_load_id AND _LOADED_AT != _UPDATED_AT;

    v_dup_count := v_valid_count - v_inserted_count - v_updated_count;
    IF (v_dup_count < 0) THEN
        v_dup_count := 0;
    END IF;

    -- Finalize load tracking
    UPDATE DE_POC.PUBLIC.CUSTOMER_LOAD_TRACKING
    SET RECORDS_VALID = :v_valid_count, RECORDS_REJECTED = :v_rejected_count,
        RECORDS_INSERTED = :v_inserted_count, RECORDS_UPDATED = :v_updated_count,
        RECORDS_DUPLICATE = :v_dup_count, PROCESSING_STATUS = 'SUCCESS',
        COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE LOAD_ID = :v_load_id;

    DROP TABLE IF EXISTS DE_POC.PUBLIC.TEMP_CUSTOMER_BATCH;

    RETURN OBJECT_CONSTRUCT(
        'load_id', :v_load_id, 'source_file', :v_source_file, 'status', 'SUCCESS',
        'records_staged', :v_staged_count, 'records_valid', :v_valid_count,
        'records_rejected', :v_rejected_count, 'records_inserted', :v_inserted_count,
        'records_updated', :v_updated_count, 'records_duplicate', :v_dup_count
    );

EXCEPTION
    WHEN OTHER THEN
        UPDATE DE_POC.PUBLIC.CUSTOMER_LOAD_TRACKING
        SET PROCESSING_STATUS = 'FAILED', ERROR_MESSAGE = SQLERRM, COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE LOAD_ID = :v_load_id;

        DROP TABLE IF EXISTS DE_POC.PUBLIC.TEMP_CUSTOMER_BATCH;

        RETURN OBJECT_CONSTRUCT('load_id', :v_load_id, 'status', 'FAILED', 'error', SQLERRM);
END;
$$;