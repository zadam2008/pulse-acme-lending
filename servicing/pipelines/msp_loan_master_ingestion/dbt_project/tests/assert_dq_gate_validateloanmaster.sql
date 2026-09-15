{{ config(
    severity='error',
    tags=['pulse', 'msp_loan_master_ingestion', 'validateloanmaster', 'intermediate', 'op_engine']
) }}

WITH src AS (
    SELECT
        src.*,
        (`loan_id` IS NULL) AS _dq_c0_bad,
        (COUNT(*) OVER (PARTITION BY `loan_id`) > 1) AS _dq_c1_bad,
        (`original_loan_amount` IS NOT NULL AND ((SAFE_CAST(CAST(`original_loan_amount` AS STRING) AS FLOAT64) < 0))) AS _dq_c2_bad,
        (`current_upb` IS NOT NULL AND ((SAFE_CAST(CAST(`current_upb` AS STRING) AS FLOAT64) < 0))) AS _dq_c3_bad,
        (`interest_rate` IS NOT NULL AND ((SAFE_CAST(CAST(`interest_rate` AS STRING) AS FLOAT64) < 0) OR (SAFE_CAST(CAST(`interest_rate` AS STRING) AS FLOAT64) > 100))) AS _dq_c4_bad,
        (`loan_status` IS NOT NULL AND NOT (`loan_status` IN ('CURRENT', 'DELINQUENT', 'DEFAULT', 'FORECLOSURE', 'REO', 'PAID_OFF', 'MODIFIED', 'FORBEARANCE'))) AS _dq_c9_bad,
        (NOT (`origination_date` IS NULL AND `maturity_date` IS NULL) AND (COUNT(*) OVER (PARTITION BY `origination_date`, `maturity_date`) > 1)) AS _dq_c10_bad
    FROM {{ ref('dim__loanmasterscd2') }} AS src
),
stats AS (
    SELECT
        COUNT(*) AS _dq_total,
        COUNTIF(_dq_c0_bad) AS _dq_c0_failed,
        COUNTIF(_dq_c1_bad) AS _dq_c1_failed,
        COUNTIF(_dq_c2_bad) AS _dq_c2_failed,
        COUNTIF(_dq_c3_bad) AS _dq_c3_failed,
        COUNTIF(_dq_c4_bad) AS _dq_c4_failed,
        (COUNT(*) > 0) AS _dq_c5_ok,
        COUNTIF(_dq_c9_bad) AS _dq_c9_failed,
        COUNTIF(_dq_c10_bad) AS _dq_c10_failed
    FROM src
),
flags AS (
    SELECT
        _dq_total,
        ((IF(_dq_total = 0, 0.0, _dq_c0_failed / _dq_total)) > 0) AS _dq_c0_failed_check,
        ((IF(_dq_total = 0, 0.0, _dq_c1_failed / _dq_total)) > 0) AS _dq_c1_failed_check,
        ((IF(_dq_total = 0, 0.0, _dq_c2_failed / _dq_total)) > 0) AS _dq_c2_failed_check,
        ((IF(_dq_total = 0, 0.0, _dq_c3_failed / _dq_total)) > 0) AS _dq_c3_failed_check,
        ((IF(_dq_total = 0, 0.0, _dq_c4_failed / _dq_total)) > 0) AS _dq_c4_failed_check,
        (NOT _dq_c5_ok) AS _dq_c5_failed_check,
        TRUE AS _dq_c6_failed_check,
        TRUE AS _dq_c7_failed_check,
        TRUE AS _dq_c8_failed_check,
        ((IF(_dq_total = 0, 0.0, _dq_c9_failed / _dq_total)) > 0) AS _dq_c9_failed_check,
        ((IF(_dq_total = 0, 0.0, _dq_c10_failed / _dq_total)) > 0) AS _dq_c10_failed_check
    FROM stats
)
SELECT 'unknown' AS failed_check, 'unrecognized check \'unknown\'; refusing to pass-open (GitHub #113)' AS fail_reason
FROM flags
WHERE _dq_c6_failed_check
UNION ALL
SELECT 'unknown' AS failed_check, 'unrecognized check \'unknown\'; refusing to pass-open (GitHub #113)' AS fail_reason
FROM flags
WHERE _dq_c7_failed_check
UNION ALL
SELECT 'unknown' AS failed_check, 'unrecognized check \'unknown\'; refusing to pass-open (GitHub #113)' AS fail_reason
FROM flags
WHERE _dq_c8_failed_check
