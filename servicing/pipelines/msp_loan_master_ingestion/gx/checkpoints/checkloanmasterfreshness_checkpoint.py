# Generated GX checkpoint for CheckLoanMasterFreshness
# Blueprint: FreshnessChecks
# Codegen engine: CodegenOpEngine

import os
from pyspark.sql import SparkSession

PULSE_BUSINESS_DATE = os.environ.get('PULSE_BUSINESS_DATE', '{{ ds }}')
import json as _pulse_json
import sys as _pulse_sys


def _pulse_diag(step, event, fields=None):
    _rec = {"v": 1, "step": step, "event": event}
    if fields:
        _rec.update(fields)
    _pulse_sys.stdout.write(
        "PULSE_DIAG " + _pulse_json.dumps(_rec, default=str, separators=(",", ":")) + "\n")
    _pulse_sys.stdout.flush()

_pulse_diag("checkloanmasterfreshness", "task_start", {"pipeline": "msp_loan_master_ingestion", "instance": "CheckLoanMasterFreshness", "blueprint": "FreshnessChecks", "mode": "GCP_PULSE", "layer": "silver", "ops": ["check-data", "emit-report"], "business_date": PULSE_BUSINESS_DATE, "run_id": os.environ.get('PULSE_RUN_ID', ''), "airflow_task_id": os.environ.get('PULSE_TASK_ID', '')})

spark = SparkSession.builder.appName('gx_checkloanmasterfreshness').getOrCreate()
df = spark.read.format('bigquery').option('table', 'wf-pulse-agentic-dev2.pulse_silver.msp_loan_master_ingestion__cleanloanmaster').load()
report_df = df
# check-data: freshness SLA check over 'df' (no runtime GX); builds report_df.
import os
from pyspark.sql import functions as F
timestamp_column = 'last_payment_date'
max_age_minutes = 1440
business_date = os.environ.get('PULSE_BUSINESS_DATE', '1970-01-01')
_freshness_required_cols = {'dataset_name', 'last_loaded_at', 'max_age_hours', 'evaluated_at'}
if _freshness_required_cols.issubset(set(df.columns)):
    _per_bureau = df.groupBy('dataset_name').agg(
        F.max(F.to_timestamp(F.col('last_loaded_at'))).alias('last_loaded_at'),
        F.max(F.col('max_age_hours').cast('double')).alias('_max_age_hours'),
        F.max(F.to_timestamp(F.col('evaluated_at'))).alias('evaluated_at')
    )
    _per_bureau = _per_bureau.withColumn(
        'age_hours',
        (F.unix_timestamp(F.col('evaluated_at')) - F.unix_timestamp(F.col('last_loaded_at'))) / F.lit(3600.0)
    )
    _per_bureau = _per_bureau.withColumn(
        'breach_flag',
        F.col('last_loaded_at').isNull() | (F.col('age_hours') > F.col('_max_age_hours'))
    ).withColumn(
        'status',
        F.when(F.col('breach_flag'), F.lit('BREACH')).otherwise(F.lit('PASS'))
    )
    _pulse_run_id = os.environ.get('PULSE_RUN_ID', os.environ.get('AIRFLOW_CTX_DAG_RUN_ID', 'unknown'))
    report_df = _per_bureau.select(
        'dataset_name',
        'last_loaded_at',
        F.col('age_hours').cast('double').alias('age_hours'),
        'status',
        F.col('breach_flag').cast('boolean').alias('breach_flag'),
        'evaluated_at',
        F.lit(_pulse_run_id).alias('_pulse_run_id')
    )
    _breach_count = report_df.filter(F.col('breach_flag')).count()
    status = 'FAIL' if _breach_count > 0 else 'PASS'
    print(f"check-data freshness: per-bureau rows={report_df.count()} breaches={_breach_count}")
    _pulse_diag("checkloanmasterfreshness", "freshness", {"op": "check-data", "shape": "per_dataset", "datasets": report_df.count(), "breaches": _breach_count, "expected_date": business_date, "max_age_minutes": 1440, "resolved_on_failure": "block"})
    def _pulse_emit_freshness_alert(row):
        print('PULSE_FRESHNESS_ALERT dataset=' + str(row['dataset_name']) + ' status=' + str(row['status']) + ' evaluated_at=' + str(row['evaluated_at']))
    for _pulse_breach in report_df.filter(F.col('breach_flag')).limit(100).collect():
        _pulse_emit_freshness_alert(_pulse_breach)
    if status == 'FAIL':
        raise Exception("check-data: freshness check failed (on_failure=block).")
else:
    _fr_stats = df.select(
        F.count(F.lit(1)).cast('long').alias('row_count'),
        F.date_format(F.max(F.to_date(F.col(timestamp_column))), 'yyyy-MM-dd').alias('max_observed_date')
    ).collect()[0]
    max_observed_date = _fr_stats['max_observed_date']
    if max_observed_date is None:
        actual_age_minutes = None
        status = 'BREACH'
    else:
        actual_age_minutes = spark.sql(
            f"SELECT CAST((unix_timestamp(to_date('{business_date}')) - unix_timestamp(to_date('{max_observed_date}'))) / 60 AS BIGINT) AS age_minutes"
        ).collect()[0]['age_minutes']
        status = 'PASS' if actual_age_minutes <= max_age_minutes else 'BREACH'
    breach_flag = status == 'BREACH'
    _pulse_run_id = os.environ.get('PULSE_RUN_ID', os.environ.get('AIRFLOW_CTX_DAG_RUN_ID', 'unknown'))
    dataset_name = os.environ.get('PULSE_DATASET_NAME', 'freshness_dataset')
    _pulse_freshness_result = {
        'dataset_name': dataset_name,
        'expected_by': business_date,
        'actual_arrival': max_observed_date,
        'status': status,
        'breach_flag': bool(breach_flag),
        'evaluated_at': business_date,
        '_pulse_run_id': _pulse_run_id,
    }
    from pyspark.sql.types import StructType as _PulseStructType, StructField as _PulseStructField, StringType as _PulseStringType, BooleanType as _PulseBooleanType
    _pulse_freshness_schema = _PulseStructType([
        _PulseStructField('dataset_name', _PulseStringType(), False),
        _PulseStructField('expected_by', _PulseStringType(), False),
        _PulseStructField('actual_arrival', _PulseStringType(), True),
        _PulseStructField('status', _PulseStringType(), False),
        _PulseStructField('breach_flag', _PulseBooleanType(), False),
        _PulseStructField('evaluated_at', _PulseStringType(), False),
        _PulseStructField('_pulse_run_id', _PulseStringType(), False),
    ])
    report_df = spark.createDataFrame([_pulse_freshness_result], _pulse_freshness_schema)
    def _pulse_emit_freshness_alert(row):
        print('PULSE_FRESHNESS_ALERT dataset=' + str(row['dataset_name']) + ' status=' + str(row['status']) + ' evaluated_at=' + str(row['evaluated_at']))
    if breach_flag:
        _pulse_emit_freshness_alert(_pulse_freshness_result)
        raise Exception("check-data: freshness breach (on_failure=block).")
    print(f"check-data freshness: status={status} breach={breach_flag} actual_age_minutes={actual_age_minutes} max={max_age_minutes}")
    _pulse_diag("checkloanmasterfreshness", "freshness", {"op": "check-data", "timestamp_column": "last_payment_date", "dataset": dataset_name, "row_count": _fr_stats['row_count'], "max_observed_date": max_observed_date, "expected_date": business_date, "actual_age_minutes": actual_age_minutes, "max_age_minutes": 1440, "status": status, "resolved_on_failure": "block", "remediation": "an empty table reports row_count 0 and a null max_observed_date; a stale one reports rows with an old date"})
# emit-report: write the DQ report (FIX #7: append by default; Mode-aware catalog write).
# Mode=GCP_PULSE, layer=silver, format=iceberg, report_mode=overwrite
import os
report_path = os.environ.get('PULSE_REPORT_URI', 'gs://pulse-home-lending-dev-lake/servicing/msp/msp-loan-master-ingestion/silver/msp_loan_master_ingestion_checkloanmasterfreshness')
import re as _pulse_re
_pulse_report_table = 'pulse.pulse_silver.' + _pulse_re.sub(r'[^A-Za-z0-9_]', '_', report_path.rstrip('/').split('/')[-1])
report_df.writeTo(_pulse_report_table).using('iceberg').createOrReplace()
print(f"emit-report: {report_df.count()} rows written to {_pulse_report_table}")
output_path = 'gs://pulse-home-lending-dev-lake/servicing/msp/msp-loan-master-ingestion/silver/msp_loan_master_ingestion_checkloanmasterfreshness'
spark.stop()
