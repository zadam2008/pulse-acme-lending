# Generated GX checkpoint for DetectLoanMasterDrift
# Blueprint: SchemaDriftDetection
# Codegen engine: CodegenOpEngine

import os
from pyspark.sql import SparkSession

PULSE_BUSINESS_DATE = os.environ.get('PULSE_BUSINESS_DATE', '{{ ds }}')
spark = SparkSession.builder.appName('gx_detectloanmasterdrift').getOrCreate()
df = spark.table('pulse.pulse_bronze.ingestloanmaster')
report_df = df
# check-data: schema-drift check over 'df' (no runtime GX); builds report_df.
expected_columns = ['loan_id', 'loan_number', 'servicer_name', 'investor_name', 'loan_type', 'loan_purpose', 'loan_status', 'origination_date', 'maturity_date', 'original_loan_amount', 'current_upb', 'interest_rate', 'rate_type', 'arm_margin', 'arm_rate_cap', 'arm_rate_floor', 'next_rate_adjustment_date', 'original_loan_term_months', 'remaining_term_months', 'pi_payment', 'escrow_payment_monthly', 'total_monthly_payment', 'payment_frequency', 'escrow_status', 'escrow_balance', 'last_payment_date', 'next_payment_due_date', 'months_delinquent', 'ltv_ratio', 'cltv_ratio', 'appraised_value', 'property_type', 'occupancy_type', 'property_address_line1', 'property_city', 'property_state', 'property_zip', 'property_county', 'number_of_units', 'year_built', 'tax_amount_annual', 'insurance_type', 'insurance_premium_annual', 'flood_zone', 'flood_insurance_required', 'flood_insurance_annual', 'has_pmi', 'pmi_monthly_premium', 'pmi_provider', 'origination_channel', 'documentation_type', 'borrower_first_name', 'borrower_last_name', 'borrower_ssn', 'borrower_dob', 'borrower_email', 'borrower_phone', 'borrower_marital_status', 'borrower_employment_status', 'borrower_annual_income', 'borrower_credit_score', 'borrower_dti_ratio', 'borrower_bankruptcy_history', 'co_borrower_first_name', 'co_borrower_last_name', 'co_borrower_relationship', 'co_borrower_credit_score', 'co_borrower_annual_income', 'modification_type', 'modification_date', 'late_charges_due', 'suspense_balance', 'partial_payment_balance', 'last_inspection_date', 'mers_registered', 'mers_min_number', 'investor_loan_id', 'boarding_date']
allow_extra_columns = True
actual_columns = [c for c in df.columns if not c.startswith('_pulse_')]
missing_columns = sorted([c for c in expected_columns if c not in actual_columns])
added_columns = sorted([c for c in actual_columns if c not in expected_columns])
status = 'PASS' if not missing_columns and (allow_extra_columns or not added_columns) else 'FAIL'
report_df = spark.createDataFrame([{
    'check_name': 'schema_drift',
    'expected_columns': '|'.join(expected_columns),
    'actual_columns': '|'.join(actual_columns),
    'missing_columns': '|'.join(missing_columns),
    'added_columns': '|'.join(added_columns),
    'allow_extra_columns': 'true' if allow_extra_columns else 'false',
    'expected_column_count': len(expected_columns),
    'actual_column_count': len(actual_columns),
    'row_count': df.count(),
    'status': status,
}])
print(f"check-data schema_drift: status={status} missing={missing_columns} added={added_columns}")
if status == 'FAIL':
    print("check-data WARNING: schema_drift check failed but on_failure=warn; continuing.")
# emit-report: write the DQ report (FIX #7: append by default; Mode-aware catalog write).
# Mode=GCP_PULSE, layer=silver, format=iceberg, report_mode=append
import os
report_path = os.environ.get('PULSE_REPORT_URI', 'gs://pulse-home-lending-dev-lake/servicing/msp/msp-loan-master-ingestion/silver/msp_loan_master_ingestion_detectloanmasterdrift')
import re as _pulse_re
_pulse_report_table = 'pulse.pulse_silver.' + _pulse_re.sub(r'[^A-Za-z0-9_]', '_', report_path.rstrip('/').split('/')[-1])
if spark.catalog.tableExists(_pulse_report_table):
    report_df.writeTo(_pulse_report_table).append()
else:
    report_df.writeTo(_pulse_report_table).using('iceberg').create()
print(f"emit-report: {report_df.count()} rows written to {_pulse_report_table}")
output_path = 'gs://pulse-home-lending-dev-lake/servicing/msp/msp-loan-master-ingestion/silver/msp_loan_master_ingestion_detectloanmasterdrift'
spark.stop()
