{{ config(
    severity='error',
    tags=['pulse', 'msp_loan_master_ingestion', 'validateloanmaster', 'intermediate', 'op_engine']
) }}

WITH src AS (
    SELECT
        src.*,
        (`loan_id` IS NULL) AS _dq_c0_bad,
        (COUNT(*) OVER (PARTITION BY `loan_id`) > 1) AS _dq_c1_bad,
        (`loan_status` IS NULL) AS _dq_c2_bad,
        (`loan_status` IS NOT NULL AND NOT (`loan_status` IN ('ACTIVE', 'CLOSED', 'DEFAULT', 'FORBEARANCE', 'MODIFICATION', 'PAID_OFF'))) AS _dq_c3_bad,
        (`months_delinquent` IS NOT NULL AND ((SAFE_CAST(CAST(`months_delinquent` AS STRING) AS FLOAT64) < 0))) AS _dq_c4_bad,
        (`borrower_annual_income` IS NOT NULL AND ((SAFE_CAST(CAST(`borrower_annual_income` AS STRING) AS FLOAT64) < 0))) AS _dq_c5_bad,
        (`appraised_value` IS NOT NULL AND ((SAFE_CAST(CAST(`appraised_value` AS STRING) AS FLOAT64) < 0))) AS _dq_c6_bad,
        (NOT (`original_loan_amount` IS NULL AND `current_upb` IS NULL) AND (CASE WHEN SAFE_CAST(CAST(`original_loan_amount` AS STRING) AS FLOAT64) IS NOT NULL AND SAFE_CAST(CAST(`current_upb` AS STRING) AS FLOAT64) IS NOT NULL THEN SAFE_CAST(CAST(`original_loan_amount` AS STRING) AS FLOAT64) >= SAFE_CAST(CAST(`current_upb` AS STRING) AS FLOAT64) ELSE `original_loan_amount` >= `current_upb` END IS NOT TRUE)) AS _dq_c7_bad,
        (NOT (`maturity_date` IS NULL AND `origination_date` IS NULL) AND (CASE WHEN SAFE_CAST(CAST(`maturity_date` AS STRING) AS FLOAT64) IS NOT NULL AND SAFE_CAST(CAST(`origination_date` AS STRING) AS FLOAT64) IS NOT NULL THEN SAFE_CAST(CAST(`maturity_date` AS STRING) AS FLOAT64) > SAFE_CAST(CAST(`origination_date` AS STRING) AS FLOAT64) ELSE `maturity_date` > `origination_date` END IS NOT TRUE)) AS _dq_c8_bad,
        (NOT (`original_loan_term_months` IS NULL AND `remaining_term_months` IS NULL) AND (CASE WHEN SAFE_CAST(CAST(`original_loan_term_months` AS STRING) AS FLOAT64) IS NOT NULL AND SAFE_CAST(CAST(`remaining_term_months` AS STRING) AS FLOAT64) IS NOT NULL THEN SAFE_CAST(CAST(`original_loan_term_months` AS STRING) AS FLOAT64) >= SAFE_CAST(CAST(`remaining_term_months` AS STRING) AS FLOAT64) ELSE `original_loan_term_months` >= `remaining_term_months` END IS NOT TRUE)) AS _dq_c9_bad,
        (`ltv_ratio` IS NOT NULL AND ((SAFE_CAST(CAST(`ltv_ratio` AS STRING) AS FLOAT64) < 0) OR (SAFE_CAST(CAST(`ltv_ratio` AS STRING) AS FLOAT64) > 200))) AS _dq_c10_bad,
        (`interest_rate` IS NOT NULL AND ((SAFE_CAST(CAST(`interest_rate` AS STRING) AS FLOAT64) < 0) OR (SAFE_CAST(CAST(`interest_rate` AS STRING) AS FLOAT64) > 25))) AS _dq_c11_bad,
        (NOT (`loan_id` IS NULL AND `origination_date` IS NULL) AND (COUNT(*) OVER (PARTITION BY `loan_id`, `origination_date`) > 1)) AS _dq_c12_bad,
        (`next_rate_adjustment_date` IS NULL) AS _dq_c13_bad
    FROM {{ ref('stg__msp__maskloanmasterpii_masked') }} AS src
),
stats AS (
    SELECT
        COUNT(*) AS _dq_total,
        COUNTIF(_dq_c0_bad) AS _dq_c0_failed,
        COUNTIF(_dq_c1_bad) AS _dq_c1_failed,
        COUNTIF(_dq_c2_bad) AS _dq_c2_failed,
        COUNTIF(_dq_c3_bad) AS _dq_c3_failed,
        COUNTIF(_dq_c4_bad) AS _dq_c4_failed,
        COUNTIF(_dq_c5_bad) AS _dq_c5_failed,
        COUNTIF(_dq_c6_bad) AS _dq_c6_failed,
        COUNTIF(_dq_c7_bad) AS _dq_c7_failed,
        COUNTIF(NOT (`original_loan_amount` IS NULL AND `current_upb` IS NULL)) AS _dq_c7_evaluated,
        COUNTIF(_dq_c8_bad) AS _dq_c8_failed,
        COUNTIF(NOT (`maturity_date` IS NULL AND `origination_date` IS NULL)) AS _dq_c8_evaluated,
        COUNTIF(_dq_c9_bad) AS _dq_c9_failed,
        COUNTIF(NOT (`original_loan_term_months` IS NULL AND `remaining_term_months` IS NULL)) AS _dq_c9_evaluated,
        COUNTIF(_dq_c10_bad) AS _dq_c10_failed,
        COUNTIF(_dq_c11_bad) AS _dq_c11_failed,
        COUNTIF(_dq_c12_bad) AS _dq_c12_failed,
        COUNTIF(_dq_c13_bad) AS _dq_c13_failed
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
        ((IF(_dq_total = 0, 0.0, _dq_c5_failed / _dq_total)) > 0) AS _dq_c5_failed_check,
        ((IF(_dq_total = 0, 0.0, _dq_c6_failed / _dq_total)) > 0) AS _dq_c6_failed_check,
        ((IF(_dq_c7_evaluated = 0, 0.0, _dq_c7_failed / _dq_c7_evaluated)) > 0) AS _dq_c7_failed_check,
        ((IF(_dq_c8_evaluated = 0, 0.0, _dq_c8_failed / _dq_c8_evaluated)) > 0) AS _dq_c8_failed_check,
        ((IF(_dq_c9_evaluated = 0, 0.0, _dq_c9_failed / _dq_c9_evaluated)) > 0) AS _dq_c9_failed_check,
        ((IF(_dq_total = 0, 0.0, _dq_c10_failed / _dq_total)) > 0) AS _dq_c10_failed_check,
        ((IF(_dq_total = 0, 0.0, _dq_c11_failed / _dq_total)) > 0) AS _dq_c11_failed_check,
        ((IF(_dq_total = 0, 0.0, _dq_c12_failed / _dq_total)) > 0) AS _dq_c12_failed_check,
        ((IF(_dq_total = 0, 0.0, _dq_c13_failed / _dq_total)) > 0) AS _dq_c13_failed_check
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
SELECT 'not_null loan_status' AS failed_check, 'check-data assertion failed' AS fail_reason
FROM flags
WHERE _dq_c2_failed_check
UNION ALL
SELECT 'column_pair_compare maturity_date,origination_date' AS failed_check, 'check-data assertion failed' AS fail_reason
FROM flags
WHERE _dq_c8_failed_check
