{{ config(
    severity='error',
    tags=['pulse', 'msp_loan_master_ingestion', 'validateloanmaster', 'intermediate', 'op_engine']
) }}

-- check-data gate: no blocking checks; dbt test is a no-op.
SELECT * FROM (SELECT 'check-data' AS failed_check) WHERE FALSE
