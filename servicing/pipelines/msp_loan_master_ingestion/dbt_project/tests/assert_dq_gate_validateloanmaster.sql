{{ config(
    severity='error',
    tags=['pulse', 'msp_loan_master_ingestion', 'validateloanmaster', 'intermediate', 'op_engine']
) }}

{% do log('PULSE_DIAG {"v":1,"step":"assert_dq_gate_validateloanmaster","event":"dq_rule","rule":"ExpectColumnValuesToNotBeNull","kind":"not_null","columns":["loan_id"],"expected":"value is not null","authored_on_failure":"block","resolved_on_failure":"block","mostly":1.0,"fail_closed":false,"idx":0,"row_level":true}', info=True) %}
{% do log('PULSE_DIAG {"v":1,"step":"assert_dq_gate_validateloanmaster","event":"dq_rule","rule":"ExpectColumnValuesToBeUnique","kind":"unique","columns":["loan_id"],"expected":"value is unique","authored_on_failure":"block","resolved_on_failure":"block","mostly":1.0,"fail_closed":false,"idx":1,"row_level":true}', info=True) %}
{% do log('PULSE_DIAG {"v":1,"step":"assert_dq_gate_validateloanmaster","event":"dq_rule","rule":"ExpectColumnValuesToNotBeNull","kind":"not_null","columns":["loan_number"],"expected":"value is not null","authored_on_failure":"block","resolved_on_failure":"block","mostly":1.0,"fail_closed":false,"idx":2,"row_level":true}', info=True) %}
{% do log('PULSE_DIAG {"v":1,"step":"assert_dq_gate_validateloanmaster","event":"dq_rule","rule":"ExpectColumnValuesToBeUnique","kind":"unique","columns":["loan_number"],"expected":"value is unique","authored_on_failure":"block","resolved_on_failure":"block","mostly":1.0,"fail_closed":false,"idx":3,"row_level":true}', info=True) %}
{% do log('PULSE_DIAG {"v":1,"step":"assert_dq_gate_validateloanmaster","event":"dq_rule","rule":"ExpectColumnValuesToBeBetween","kind":"between","columns":["original_loan_amount"],"expected":"value >= 0","authored_on_failure":"block","resolved_on_failure":"block","mostly":1.0,"fail_closed":false,"idx":4,"row_level":true}', info=True) %}
{% do log('PULSE_DIAG {"v":1,"step":"assert_dq_gate_validateloanmaster","event":"dq_rule","rule":"ExpectColumnValuesToBeBetween","kind":"between","columns":["current_upb"],"expected":"value >= 0","authored_on_failure":"block","resolved_on_failure":"block","mostly":1.0,"fail_closed":false,"idx":5,"row_level":true}', info=True) %}
{% do log('PULSE_DIAG {"v":1,"step":"assert_dq_gate_validateloanmaster","event":"dq_rule","rule":"ExpectColumnValuesToBeBetween","kind":"between","columns":["months_delinquent"],"expected":"value >= 0","authored_on_failure":"warn","resolved_on_failure":"warn","mostly":1.0,"fail_closed":false,"idx":6,"row_level":true}', info=True) %}
{% do log('PULSE_DIAG {"v":1,"step":"assert_dq_gate_validateloanmaster","event":"dq_rule","rule":"ExpectColumnValuesToBeBetween","kind":"between","columns":["escrow_balance"],"expected":"value >= 0","authored_on_failure":"warn","resolved_on_failure":"warn","mostly":1.0,"fail_closed":false,"idx":7,"row_level":true}', info=True) %}
{% do log('PULSE_DIAG {"v":1,"step":"assert_dq_gate_validateloanmaster","event":"dq_rule","rule":"ExpectColumnValuesToBeBetween","kind":"between","columns":["interest_rate"],"expected":"value between 0 and 100","authored_on_failure":"warn","resolved_on_failure":"warn","mostly":1.0,"fail_closed":false,"idx":8,"row_level":true}', info=True) %}
{% do log('PULSE_DIAG {"v":1,"step":"assert_dq_gate_validateloanmaster","event":"dq_rule","rule":"ExpectColumnValuesToBeBetween","kind":"between","columns":["ltv_ratio"],"expected":"value between 0 and 200","authored_on_failure":"warn","resolved_on_failure":"warn","mostly":1.0,"fail_closed":false,"idx":9,"row_level":true}', info=True) %}
{% do log('PULSE_DIAG {"v":1,"step":"assert_dq_gate_validateloanmaster","event":"dq_rule","rule":"ExpectColumnValuesToBeBetween","kind":"between","columns":["borrower_credit_score"],"expected":"value between 300 and 850","authored_on_failure":"warn","resolved_on_failure":"warn","mostly":1.0,"fail_closed":false,"idx":10,"row_level":true}', info=True) %}
{% do log('PULSE_DIAG {"v":1,"step":"assert_dq_gate_validateloanmaster","event":"dq_rule","rule":"ExpectColumnPairValuesAToBeGreaterThanB","kind":"column_pair_compare","columns":["maturity_date","origination_date"],"expected":"maturity_date > origination_date","authored_on_failure":"block","resolved_on_failure":"block","mostly":1.0,"fail_closed":false,"idx":11,"row_level":true}', info=True) %}
{% do log('PULSE_DIAG {"v":1,"step":"assert_dq_gate_validateloanmaster","event":"dq_rule","rule":"ExpectColumnPairValuesAToBeGreaterThanB","kind":"column_pair_compare","columns":["original_loan_amount","current_upb"],"expected":"original_loan_amount > current_upb","authored_on_failure":"warn","resolved_on_failure":"warn","mostly":1.0,"fail_closed":false,"idx":12,"row_level":true}', info=True) %}
{% do log('PULSE_DIAG {"v":1,"step":"assert_dq_gate_validateloanmaster","event":"dq_rule","rule":"ExpectColumnPairValuesAToBeGreaterThanB","kind":"column_pair_compare","columns":["next_payment_due_date","last_payment_date"],"expected":"next_payment_due_date > last_payment_date","authored_on_failure":"warn","resolved_on_failure":"warn","mostly":1.0,"fail_closed":false,"idx":13,"row_level":true}', info=True) %}
{% do log('PULSE_DIAG {"v":1,"step":"assert_dq_gate_validateloanmaster","event":"dq_unsupported","rule":"ExpectMulticolumnValuesToBeUnique","kind":"unknown","columns":["loan_number","origination_date"],"expected":"unknown","authored_on_failure":"block","resolved_on_failure":"block","mostly":1.0,"fail_closed":true,"idx":14,"row_level":false,"reason":"unrecognized check \'unknown\'; refusing to pass-open (GitHub #113)","remediation":"rule type is not in the supported vocabulary; re-author it as a supported GX type or set its severity to warn. Supported types: CheckDataExpectationParser."}', info=True) %}
{% do log('PULSE_DIAG {"v":1,"step":"assert_dq_gate_validateloanmaster","event":"dq_rule","rule":"ExpectColumnPairValuesAToBeGreaterThanB","kind":"column_pair_compare","columns":["appraised_value","original_loan_amount"],"expected":"appraised_value > original_loan_amount","authored_on_failure":"warn","resolved_on_failure":"warn","mostly":1.0,"fail_closed":false,"idx":15,"row_level":true}', info=True) %}
{% do log('PULSE_DIAG {"v":1,"step":"assert_dq_gate_validateloanmaster","event":"dq_gate","compiled_rules":16,"blocking_rules":8,"quarantine":true}', info=True) %}

WITH src AS (
    SELECT
        src.*,
        (`loan_id` IS NULL) AS _dq_c0_bad,
        (COUNT(*) OVER (PARTITION BY `loan_id`) > 1) AS _dq_c1_bad,
        (`loan_number` IS NULL) AS _dq_c2_bad,
        (COUNT(*) OVER (PARTITION BY `loan_number`) > 1) AS _dq_c3_bad,
        (`original_loan_amount` IS NOT NULL AND ((SAFE_CAST(CAST(`original_loan_amount` AS STRING) AS FLOAT64) < 0))) AS _dq_c4_bad,
        (`current_upb` IS NOT NULL AND ((SAFE_CAST(CAST(`current_upb` AS STRING) AS FLOAT64) < 0))) AS _dq_c5_bad,
        (`months_delinquent` IS NOT NULL AND ((SAFE_CAST(CAST(`months_delinquent` AS STRING) AS FLOAT64) < 0))) AS _dq_c6_bad,
        (`escrow_balance` IS NOT NULL AND ((SAFE_CAST(CAST(`escrow_balance` AS STRING) AS FLOAT64) < 0))) AS _dq_c7_bad,
        (`interest_rate` IS NOT NULL AND ((SAFE_CAST(CAST(`interest_rate` AS STRING) AS FLOAT64) < 0) OR (SAFE_CAST(CAST(`interest_rate` AS STRING) AS FLOAT64) > 100))) AS _dq_c8_bad,
        (`ltv_ratio` IS NOT NULL AND ((SAFE_CAST(CAST(`ltv_ratio` AS STRING) AS FLOAT64) < 0) OR (SAFE_CAST(CAST(`ltv_ratio` AS STRING) AS FLOAT64) > 200))) AS _dq_c9_bad,
        (`borrower_credit_score` IS NOT NULL AND ((SAFE_CAST(CAST(`borrower_credit_score` AS STRING) AS FLOAT64) < 300) OR (SAFE_CAST(CAST(`borrower_credit_score` AS STRING) AS FLOAT64) > 850))) AS _dq_c10_bad,
        (NOT (`maturity_date` IS NULL AND `origination_date` IS NULL) AND (CASE WHEN SAFE_CAST(CAST(`maturity_date` AS STRING) AS FLOAT64) IS NOT NULL AND SAFE_CAST(CAST(`origination_date` AS STRING) AS FLOAT64) IS NOT NULL THEN SAFE_CAST(CAST(`maturity_date` AS STRING) AS FLOAT64) > SAFE_CAST(CAST(`origination_date` AS STRING) AS FLOAT64) ELSE `maturity_date` > `origination_date` END IS NOT TRUE)) AS _dq_c11_bad,
        (NOT (`original_loan_amount` IS NULL AND `current_upb` IS NULL) AND (CASE WHEN SAFE_CAST(CAST(`original_loan_amount` AS STRING) AS FLOAT64) IS NOT NULL AND SAFE_CAST(CAST(`current_upb` AS STRING) AS FLOAT64) IS NOT NULL THEN SAFE_CAST(CAST(`original_loan_amount` AS STRING) AS FLOAT64) > SAFE_CAST(CAST(`current_upb` AS STRING) AS FLOAT64) ELSE `original_loan_amount` > `current_upb` END IS NOT TRUE)) AS _dq_c12_bad,
        (NOT (`next_payment_due_date` IS NULL AND `last_payment_date` IS NULL) AND (CASE WHEN SAFE_CAST(CAST(`next_payment_due_date` AS STRING) AS FLOAT64) IS NOT NULL AND SAFE_CAST(CAST(`last_payment_date` AS STRING) AS FLOAT64) IS NOT NULL THEN SAFE_CAST(CAST(`next_payment_due_date` AS STRING) AS FLOAT64) > SAFE_CAST(CAST(`last_payment_date` AS STRING) AS FLOAT64) ELSE `next_payment_due_date` > `last_payment_date` END IS NOT TRUE)) AS _dq_c13_bad,
        (NOT (`appraised_value` IS NULL AND `original_loan_amount` IS NULL) AND (CASE WHEN SAFE_CAST(CAST(`appraised_value` AS STRING) AS FLOAT64) IS NOT NULL AND SAFE_CAST(CAST(`original_loan_amount` AS STRING) AS FLOAT64) IS NOT NULL THEN SAFE_CAST(CAST(`appraised_value` AS STRING) AS FLOAT64) > SAFE_CAST(CAST(`original_loan_amount` AS STRING) AS FLOAT64) ELSE `appraised_value` > `original_loan_amount` END IS NOT TRUE)) AS _dq_c15_bad
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
        COUNTIF(_dq_c5_bad) AS _dq_c5_failed,
        COUNTIF(_dq_c6_bad) AS _dq_c6_failed,
        COUNTIF(_dq_c7_bad) AS _dq_c7_failed,
        COUNTIF(_dq_c8_bad) AS _dq_c8_failed,
        COUNTIF(_dq_c9_bad) AS _dq_c9_failed,
        COUNTIF(_dq_c10_bad) AS _dq_c10_failed,
        COUNTIF(_dq_c11_bad) AS _dq_c11_failed,
        COUNTIF(NOT (`maturity_date` IS NULL AND `origination_date` IS NULL)) AS _dq_c11_evaluated,
        COUNTIF(_dq_c12_bad) AS _dq_c12_failed,
        COUNTIF(NOT (`original_loan_amount` IS NULL AND `current_upb` IS NULL)) AS _dq_c12_evaluated,
        COUNTIF(_dq_c13_bad) AS _dq_c13_failed,
        COUNTIF(NOT (`next_payment_due_date` IS NULL AND `last_payment_date` IS NULL)) AS _dq_c13_evaluated,
        COUNTIF(_dq_c15_bad) AS _dq_c15_failed,
        COUNTIF(NOT (`appraised_value` IS NULL AND `original_loan_amount` IS NULL)) AS _dq_c15_evaluated
    FROM src
),
flags AS (
    SELECT
        _dq_total,
        _dq_c0_failed,
        _dq_c1_failed,
        _dq_c2_failed,
        _dq_c3_failed,
        _dq_c4_failed,
        _dq_c5_failed,
        _dq_c6_failed,
        _dq_c7_failed,
        _dq_c8_failed,
        _dq_c9_failed,
        _dq_c10_failed,
        _dq_c11_failed,
        _dq_c11_evaluated,
        _dq_c12_failed,
        _dq_c12_evaluated,
        _dq_c13_failed,
        _dq_c13_evaluated,
        _dq_c15_failed,
        _dq_c15_evaluated,
        ((IF(_dq_total = 0, 0.0, _dq_c0_failed / _dq_total)) > 0) AS _dq_c0_failed_check,
        ((IF(_dq_total = 0, 0.0, _dq_c1_failed / _dq_total)) > 0) AS _dq_c1_failed_check,
        ((IF(_dq_total = 0, 0.0, _dq_c2_failed / _dq_total)) > 0) AS _dq_c2_failed_check,
        ((IF(_dq_total = 0, 0.0, _dq_c3_failed / _dq_total)) > 0) AS _dq_c3_failed_check,
        ((IF(_dq_total = 0, 0.0, _dq_c4_failed / _dq_total)) > 0) AS _dq_c4_failed_check,
        ((IF(_dq_total = 0, 0.0, _dq_c5_failed / _dq_total)) > 0) AS _dq_c5_failed_check,
        ((IF(_dq_total = 0, 0.0, _dq_c6_failed / _dq_total)) > 0) AS _dq_c6_failed_check,
        ((IF(_dq_total = 0, 0.0, _dq_c7_failed / _dq_total)) > 0) AS _dq_c7_failed_check,
        ((IF(_dq_total = 0, 0.0, _dq_c8_failed / _dq_total)) > 0) AS _dq_c8_failed_check,
        ((IF(_dq_total = 0, 0.0, _dq_c9_failed / _dq_total)) > 0) AS _dq_c9_failed_check,
        ((IF(_dq_total = 0, 0.0, _dq_c10_failed / _dq_total)) > 0) AS _dq_c10_failed_check,
        ((IF(_dq_c11_evaluated = 0, 0.0, _dq_c11_failed / _dq_c11_evaluated)) > 0) AS _dq_c11_failed_check,
        ((IF(_dq_c12_evaluated = 0, 0.0, _dq_c12_failed / _dq_c12_evaluated)) > 0) AS _dq_c12_failed_check,
        ((IF(_dq_c13_evaluated = 0, 0.0, _dq_c13_failed / _dq_c13_evaluated)) > 0) AS _dq_c13_failed_check,
        TRUE AS _dq_c14_failed_check,
        ((IF(_dq_c15_evaluated = 0, 0.0, _dq_c15_failed / _dq_c15_evaluated)) > 0) AS _dq_c15_failed_check
    FROM stats
),
_failing AS (
    SELECT 0 AS _dq_gate_idx, CONCAT('PULSE_DIAG ', TO_JSON_STRING(STRUCT(1 AS v, 'assert_dq_gate_validateloanmaster' AS step, 'dq_check' AS event, 'ExpectColumnValuesToNotBeNull' AS rule, 'not_null' AS kind, ['loan_id'] AS columns, 'value is not null' AS expected, 'block' AS authored_on_failure, 'block' AS resolved_on_failure, 1.0 AS mostly, false AS fail_closed, 0 AS idx, _dq_c0_failed AS failed_rows, _dq_total AS evaluated_rows, _dq_total AS total_rows, 'compare the expected condition against the actual values; either the rule is wrong for this data or the data is wrong for this rule.' AS remediation, CONCAT('DQ FAIL ExpectColumnValuesToNotBeNull loan_id: ', CAST(_dq_c0_failed AS STRING), ' rows, expected value is not null') AS message))) AS diagnostic
    FROM flags
    WHERE _dq_c0_failed_check
    UNION ALL
    SELECT 1 AS _dq_gate_idx, CONCAT('PULSE_DIAG ', TO_JSON_STRING(STRUCT(1 AS v, 'assert_dq_gate_validateloanmaster' AS step, 'dq_check' AS event, 'ExpectColumnValuesToBeUnique' AS rule, 'unique' AS kind, ['loan_id'] AS columns, 'value is unique' AS expected, 'block' AS authored_on_failure, 'block' AS resolved_on_failure, 1.0 AS mostly, false AS fail_closed, 1 AS idx, _dq_c1_failed AS failed_rows, _dq_total AS evaluated_rows, _dq_total AS total_rows, 'compare the expected condition against the actual values; either the rule is wrong for this data or the data is wrong for this rule.' AS remediation, CONCAT('DQ FAIL ExpectColumnValuesToBeUnique loan_id: ', CAST(_dq_c1_failed AS STRING), ' rows, expected value is unique') AS message))) AS diagnostic
    FROM flags
    WHERE _dq_c1_failed_check
    UNION ALL
    SELECT 2 AS _dq_gate_idx, CONCAT('PULSE_DIAG ', TO_JSON_STRING(STRUCT(1 AS v, 'assert_dq_gate_validateloanmaster' AS step, 'dq_check' AS event, 'ExpectColumnValuesToNotBeNull' AS rule, 'not_null' AS kind, ['loan_number'] AS columns, 'value is not null' AS expected, 'block' AS authored_on_failure, 'block' AS resolved_on_failure, 1.0 AS mostly, false AS fail_closed, 2 AS idx, _dq_c2_failed AS failed_rows, _dq_total AS evaluated_rows, _dq_total AS total_rows, 'compare the expected condition against the actual values; either the rule is wrong for this data or the data is wrong for this rule.' AS remediation, CONCAT('DQ FAIL ExpectColumnValuesToNotBeNull loan_number: ', CAST(_dq_c2_failed AS STRING), ' rows, expected value is not null') AS message))) AS diagnostic
    FROM flags
    WHERE _dq_c2_failed_check
    UNION ALL
    SELECT 3 AS _dq_gate_idx, CONCAT('PULSE_DIAG ', TO_JSON_STRING(STRUCT(1 AS v, 'assert_dq_gate_validateloanmaster' AS step, 'dq_check' AS event, 'ExpectColumnValuesToBeUnique' AS rule, 'unique' AS kind, ['loan_number'] AS columns, 'value is unique' AS expected, 'block' AS authored_on_failure, 'block' AS resolved_on_failure, 1.0 AS mostly, false AS fail_closed, 3 AS idx, _dq_c3_failed AS failed_rows, _dq_total AS evaluated_rows, _dq_total AS total_rows, 'compare the expected condition against the actual values; either the rule is wrong for this data or the data is wrong for this rule.' AS remediation, CONCAT('DQ FAIL ExpectColumnValuesToBeUnique loan_number: ', CAST(_dq_c3_failed AS STRING), ' rows, expected value is unique') AS message))) AS diagnostic
    FROM flags
    WHERE _dq_c3_failed_check
    UNION ALL
    SELECT 4 AS _dq_gate_idx, CONCAT('PULSE_DIAG ', TO_JSON_STRING(STRUCT(1 AS v, 'assert_dq_gate_validateloanmaster' AS step, 'dq_check' AS event, 'ExpectColumnValuesToBeBetween' AS rule, 'between' AS kind, ['original_loan_amount'] AS columns, 'value >= 0' AS expected, 'block' AS authored_on_failure, 'block' AS resolved_on_failure, 1.0 AS mostly, false AS fail_closed, 4 AS idx, _dq_c4_failed AS failed_rows, _dq_total AS evaluated_rows, _dq_total AS total_rows, 'compare the expected condition against the actual values; either the rule is wrong for this data or the data is wrong for this rule.' AS remediation, CONCAT('DQ FAIL ExpectColumnValuesToBeBetween original_loan_amount: ', CAST(_dq_c4_failed AS STRING), ' rows, expected value >= 0') AS message))) AS diagnostic
    FROM flags
    WHERE _dq_c4_failed_check
    UNION ALL
    SELECT 5 AS _dq_gate_idx, CONCAT('PULSE_DIAG ', TO_JSON_STRING(STRUCT(1 AS v, 'assert_dq_gate_validateloanmaster' AS step, 'dq_check' AS event, 'ExpectColumnValuesToBeBetween' AS rule, 'between' AS kind, ['current_upb'] AS columns, 'value >= 0' AS expected, 'block' AS authored_on_failure, 'block' AS resolved_on_failure, 1.0 AS mostly, false AS fail_closed, 5 AS idx, _dq_c5_failed AS failed_rows, _dq_total AS evaluated_rows, _dq_total AS total_rows, 'compare the expected condition against the actual values; either the rule is wrong for this data or the data is wrong for this rule.' AS remediation, CONCAT('DQ FAIL ExpectColumnValuesToBeBetween current_upb: ', CAST(_dq_c5_failed AS STRING), ' rows, expected value >= 0') AS message))) AS diagnostic
    FROM flags
    WHERE _dq_c5_failed_check
    UNION ALL
    SELECT 11 AS _dq_gate_idx, CONCAT('PULSE_DIAG ', TO_JSON_STRING(STRUCT(1 AS v, 'assert_dq_gate_validateloanmaster' AS step, 'dq_check' AS event, 'ExpectColumnPairValuesAToBeGreaterThanB' AS rule, 'column_pair_compare' AS kind, ['maturity_date', 'origination_date'] AS columns, 'maturity_date > origination_date' AS expected, 'block' AS authored_on_failure, 'block' AS resolved_on_failure, 1.0 AS mostly, false AS fail_closed, 11 AS idx, _dq_c11_failed AS failed_rows, _dq_c11_evaluated AS evaluated_rows, _dq_total AS total_rows, 'compare the expected condition against the actual values; either the rule is wrong for this data or the data is wrong for this rule.' AS remediation, CONCAT('DQ FAIL ExpectColumnPairValuesAToBeGreaterThanB [maturity_date, origination_date]: ', CAST(_dq_c11_failed AS STRING), ' rows, expected maturity_date > origination_date') AS message))) AS diagnostic
    FROM flags
    WHERE _dq_c11_failed_check
    UNION ALL
    SELECT 14 AS _dq_gate_idx, CONCAT('PULSE_DIAG ', TO_JSON_STRING(STRUCT(1 AS v, 'assert_dq_gate_validateloanmaster' AS step, 'dq_unsupported' AS event, 'ExpectMulticolumnValuesToBeUnique' AS rule, 'unknown' AS kind, ['loan_number', 'origination_date'] AS columns, 'unknown' AS expected, 'block' AS authored_on_failure, 'block' AS resolved_on_failure, 1.0 AS mostly, true AS fail_closed, 14 AS idx, 'unrecognized check \'unknown\'; refusing to pass-open (GitHub #113)' AS reason, 'rule type is not in the supported vocabulary; re-author it as a supported GX type or set its severity to warn. Supported types: CheckDataExpectationParser.' AS remediation, 'DQ FAIL-CLOSED unsupported rule ExpectMulticolumnValuesToBeUnique [loan_number, origination_date]: unrecognized check \'unknown\'; refusing to pass-open (GitHub #113)' AS message))) AS diagnostic
    FROM flags
    WHERE _dq_c14_failed_check
)
SELECT ERROR(diagnostic_summary) AS failed_check
FROM (
    SELECT STRING_AGG(diagnostic, '\n' ORDER BY _dq_gate_idx) AS diagnostic_summary
    FROM _failing
)
WHERE EXISTS (SELECT 1 FROM _failing)
