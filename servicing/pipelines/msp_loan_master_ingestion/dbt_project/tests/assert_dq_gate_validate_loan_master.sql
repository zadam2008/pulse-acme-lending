{{ config(
    severity='error',
    tags=['pulse', 'msp_loan_master_ingestion', 'validate_loan_master', 'intermediate', 'op_engine']
) }}

WITH src AS (
    SELECT
        src.*,
        (`loan_id` IS NULL) AS _dq_c0_bad,
        (COUNT(*) OVER (PARTITION BY `loan_id`) > 1) AS _dq_c1_bad,
        (`current_upb` IS NULL) AS _dq_c2_bad,
        (`original_loan_amount` IS NOT NULL AND ((SAFE_CAST(CAST(`original_loan_amount` AS STRING) AS FLOAT64) < 0))) AS _dq_c3_bad,
        (`interest_rate` IS NOT NULL AND ((SAFE_CAST(CAST(`interest_rate` AS STRING) AS FLOAT64) < 0) OR (SAFE_CAST(CAST(`interest_rate` AS STRING) AS FLOAT64) > 100))) AS _dq_c4_bad,
        (`original_loan_amount` IS NOT NULL AND `current_upb` IS NOT NULL AND NOT (CASE WHEN SAFE_CAST(CAST(`original_loan_amount` AS STRING) AS FLOAT64) IS NOT NULL AND SAFE_CAST(CAST(`current_upb` AS STRING) AS FLOAT64) IS NOT NULL THEN SAFE_CAST(CAST(`original_loan_amount` AS STRING) AS FLOAT64) >= SAFE_CAST(CAST(`current_upb` AS STRING) AS FLOAT64) ELSE `original_loan_amount` >= `current_upb` END)) AS _dq_c7_bad,
        (`maturity_date` IS NOT NULL AND `origination_date` IS NOT NULL AND NOT (CASE WHEN SAFE_CAST(CAST(`maturity_date` AS STRING) AS FLOAT64) IS NOT NULL AND SAFE_CAST(CAST(`origination_date` AS STRING) AS FLOAT64) IS NOT NULL THEN SAFE_CAST(CAST(`maturity_date` AS STRING) AS FLOAT64) > SAFE_CAST(CAST(`origination_date` AS STRING) AS FLOAT64) ELSE `maturity_date` > `origination_date` END)) AS _dq_c8_bad,
        (`appraised_value` IS NOT NULL AND `original_loan_amount` IS NOT NULL AND NOT (CASE WHEN SAFE_CAST(CAST(`appraised_value` AS STRING) AS FLOAT64) IS NOT NULL AND SAFE_CAST(CAST(`original_loan_amount` AS STRING) AS FLOAT64) IS NOT NULL THEN SAFE_CAST(CAST(`appraised_value` AS STRING) AS FLOAT64) >= SAFE_CAST(CAST(`original_loan_amount` AS STRING) AS FLOAT64) ELSE `appraised_value` >= `original_loan_amount` END)) AS _dq_c9_bad,
        (`original_loan_term_months` IS NOT NULL AND `remaining_term_months` IS NOT NULL AND NOT (CASE WHEN SAFE_CAST(CAST(`original_loan_term_months` AS STRING) AS FLOAT64) IS NOT NULL AND SAFE_CAST(CAST(`remaining_term_months` AS STRING) AS FLOAT64) IS NOT NULL THEN SAFE_CAST(CAST(`original_loan_term_months` AS STRING) AS FLOAT64) >= SAFE_CAST(CAST(`remaining_term_months` AS STRING) AS FLOAT64) ELSE `original_loan_term_months` >= `remaining_term_months` END)) AS _dq_c10_bad,
        (NOT (`loan_id` IS NULL AND `origination_date` IS NULL) AND (COUNT(*) OVER (PARTITION BY `loan_id`, `origination_date`) > 1)) AS _dq_c11_bad
    FROM {{ ref('dim__loan_master_scd2') }} AS src
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
        COUNTIF(_dq_c7_bad) AS _dq_c7_failed,
        COUNTIF(_dq_c8_bad) AS _dq_c8_failed,
        COUNTIF(_dq_c9_bad) AS _dq_c9_failed,
        COUNTIF(_dq_c10_bad) AS _dq_c10_failed,
        COUNTIF(_dq_c11_bad) AS _dq_c11_failed
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
        ((IF(_dq_total = 0, 0.0, _dq_c7_failed / _dq_total)) > 0) AS _dq_c7_failed_check,
        ((IF(_dq_total = 0, 0.0, _dq_c8_failed / _dq_total)) > 0) AS _dq_c8_failed_check,
        ((IF(_dq_total = 0, 0.0, _dq_c9_failed / _dq_total)) > 0) AS _dq_c9_failed_check,
        ((IF(_dq_total = 0, 0.0, _dq_c10_failed / _dq_total)) > 0) AS _dq_c10_failed_check,
        ((IF(_dq_total = 0, 0.0, _dq_c11_failed / _dq_total)) > 0) AS _dq_c11_failed_check
    FROM stats
)
SELECT 'not_null loan_id' AS failed_check, 'check-data assertion failed' AS fail_reason
FROM flags
WHERE _dq_c0_failed_check
UNION ALL
SELECT 'unique loan_id' AS failed_check, 'check-data assertion failed' AS fail_reason
FROM flags
WHERE _dq_c1_failed_check
UNION ALL
SELECT 'not_null current_upb' AS failed_check, 'check-data assertion failed' AS fail_reason
FROM flags
WHERE _dq_c2_failed_check
UNION ALL
SELECT 'between original_loan_amount' AS failed_check, 'check-data assertion failed' AS fail_reason
FROM flags
WHERE _dq_c3_failed_check
UNION ALL
SELECT 'between interest_rate' AS failed_check, 'check-data assertion failed' AS fail_reason
FROM flags
WHERE _dq_c4_failed_check
UNION ALL
SELECT 'row_count_compare' AS failed_check, 'check-data assertion failed' AS fail_reason
FROM flags
WHERE _dq_c5_failed_check
UNION ALL
SELECT 'unknown' AS failed_check, 'unrecognized check \'unknown\'; refusing to pass-open (GitHub #113)' AS fail_reason
FROM flags
WHERE _dq_c6_failed_check
UNION ALL
SELECT 'column_pair_compare' AS failed_check, 'check-data assertion failed' AS fail_reason
FROM flags
WHERE _dq_c7_failed_check
UNION ALL
SELECT 'column_pair_compare' AS failed_check, 'check-data assertion failed' AS fail_reason
FROM flags
WHERE _dq_c8_failed_check
UNION ALL
SELECT 'column_pair_compare' AS failed_check, 'check-data assertion failed' AS fail_reason
FROM flags
WHERE _dq_c9_failed_check
UNION ALL
SELECT 'column_pair_compare' AS failed_check, 'check-data assertion failed' AS fail_reason
FROM flags
WHERE _dq_c10_failed_check
UNION ALL
SELECT 'compound_unique loan_id,origination_date' AS failed_check, 'check-data assertion failed' AS fail_reason
FROM flags
WHERE _dq_c11_failed_check
