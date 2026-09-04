-- Downstream consumer view for queryable customer data (AC-6)
-- Co-authored with CoCo

CREATE OR REPLACE VIEW DE_POC.PUBLIC.V_CUSTOMER_CURRENT AS
SELECT
    c.CUSTOMER_ID,
    c.FIRST_NAME,
    c.LAST_NAME,
    c.EMAIL,
    c.PHONE,
    c.ADDRESS,
    c.CITY,
    c.STATE,
    c.ZIP_CODE,
    c.COUNTRY,
    c.CREATED_DATE,
    c.UPDATED_DATE,
    c._LOADED_AT,
    c._UPDATED_AT,
    lt.SOURCE_FILE        AS LAST_SOURCE_FILE,
    lt.LOAD_TIMESTAMP     AS LAST_LOAD_TIMESTAMP,
    lt.PROCESSING_STATUS  AS LAST_LOAD_STATUS
FROM DE_POC.PUBLIC.CUSTOMER c
LEFT JOIN DE_POC.PUBLIC.CUSTOMER_LOAD_TRACKING lt
    ON c._LOAD_ID = lt.LOAD_ID;

COMMENT ON VIEW DE_POC.PUBLIC.V_CUSTOMER_CURRENT IS
    'Downstream consumer view — queryable customer data with load provenance. Supports AC-6 of DEPOC-44.';