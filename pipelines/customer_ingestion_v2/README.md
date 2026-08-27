# DEPOC-5: Customer Data Ingestion and Validation Pipeline (V2)

## Overview
Automated pipeline that ingests customer CSV data, validates records, quarantines invalid/duplicate entries, and loads valid records via idempotent MERGE.

## Architecture
```
CSV File -> @CUSTOMER_CSV_STAGE -> STG_CUSTOMERS_V2
                                       |
                          +------------+------------+
                          |                         |
                     INVALID                     VALID
                          |                         |
              QUARANTINE_CUSTOMERS         DEDUP (keep latest)
              (rejection_reason)                    |
                                          MERGE INTO CUSTOMERS_V2
                                          (INSERT new / UPDATE existing)
                                                    |
                                          PIPELINE_AUDIT_LOG
```

## NFRs
- **Idempotency**: MERGE-based upsert + file load tracking prevents duplicates
- **Restartability**: Batch-based; safe to re-run
- **Auditability**: PIPELINE_AUDIT_LOG traces every phase

## Usage
```sql
-- Process specific file
CALL SP_INGEST_CUSTOMERS_V2('customers_20240101.csv');

-- Process all files in stage
CALL SP_INGEST_CUSTOMERS_V2();
```

## Validation Rules
| Rule | Type | Field |
|------|------|-------|
| Not null/empty | MANDATORY_FIELD | CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, COUNTRY |
| Contains @ | BUSINESS_RULE | EMAIL |
| Length >= 5 | BUSINESS_RULE | EMAIL |
| Unique per batch | DUPLICATE_INTRA_BATCH | CUSTOMER_ID |
