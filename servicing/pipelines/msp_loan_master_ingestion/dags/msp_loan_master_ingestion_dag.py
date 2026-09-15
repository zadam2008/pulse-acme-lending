from airflow import DAG
from airflow.datasets import Dataset
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

def pulse_advance_time_not_implemented(**context):
    # Temporary no-op; restore AdvanceTimeDimensionOperator when issue #118 is implemented.
    import logging
    task = context.get('task')
    task_id = getattr(task, 'task_id', context.get('task_id', 'unknown'))
    logging.getLogger('pulse.advance_time').warning(
        "AdvanceTimeDimension '%s': NOT IMPLEMENTED - no time-state advance was performed. See issue #118.",
        task_id)

default_args = {
    'owner': 'pulse',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='pulse_msp_loan_master_ingestion_v1',
    description='Ingests daily MSP loan master extracts, cleans and conforms to silver, applies SCD2 history tracking, and validates data quality',
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    # PULSE re-run contract: IDEMPOTENT_OVERWRITE (source: PLATFORM_DEFAULT)
    max_active_runs=2,
    tags=['pulse', 'tenant-home-lending', 'servicing'],
) as dag:

    with TaskGroup('ingest_msp_loan_master') as tg_ingest_msp_loan_master:
        ingest_msp_loan_master = DataprocCreateBatchOperator(
            task_id='ingest_msp_loan_master',
            project_id='wf-pulse-agentic-dev2',
            region='us-central1',
            batch={
                'pyspark_batch': {'main_python_file_uri': 'gs://pulse-home-lending-dev-files/servicing/pipelines/msp_loan_master_ingestion/jobs/ingestion/ingest_msp_loan_master_ingest.py'},
                'runtime_config': {'version': '2.2', 'properties': {
                    'spark.sql.adaptive.enabled': 'true',
                    'spark.dynamicAllocation.enabled': 'true',
                    'spark.dynamicAllocation.initialExecutors': '2',
                    'spark.dynamicAllocation.minExecutors': '2',
                    'spark.jars': 'gs://pulse-home-lending-dev-files/_jars/iceberg-spark-runtime-3.5_2.13-1.6.1.jar,gs://spark-lib/bigquery/iceberg-bigquery-catalog-1.6.1-1.0.2.jar',
                    'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
                    'spark.sql.catalog.pulse': 'org.apache.iceberg.spark.SparkCatalog',
                    'spark.sql.catalog.pulse.catalog-impl': 'org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog',
                    'spark.sql.catalog.pulse.gcp_project': 'wf-pulse-agentic-dev2',
                    'spark.sql.catalog.pulse.gcp_location': 'us-central1',
                    'spark.sql.catalog.pulse.warehouse': 'gs://pulse-home-lending-dev-lake/_iceberg_warehouse',
                    'spark.dataproc.driverEnv.PULSE_TASK_ID': '{{ task.task_id }}',
                    'spark.dataproc.driverEnv.PULSE_RUN_ID': '{{ run_id }}',
                    'spark.dataproc.driverEnv.PULSE_DAG_ID': '{{ dag.dag_id }}',
                    'spark.dataproc.driverEnv.PULSE_BUSINESS_DATE': '{{ ds }}',
                    'spark.dataproc.driverEnv.PULSE_PROCESSING_TS': '{{ ts }}',
                    'spark.dataproc.driverEnv.PULSE_INGEST_TRY_NUMBER': '{{ ti.try_number }}',
                }},
                'environment_config': {'execution_config': {
                    'subnetwork_uri': 'projects/wf-pulse-agentic-dev2/regions/us-central1/subnetworks/default',
                }},
            },
        )

    with TaskGroup('clean_loan_master') as tg_clean_loan_master:
        clean_loan_master = BashOperator(
            task_id='clean_loan_master',
            bash_command="D=$(mktemp -d /tmp/dbt.XXXXXX) && gcloud storage rsync -r gs://pulse-home-lending-dev-files/servicing/pipelines/msp_loan_master_ingestion/dbt_project \"$D\" && cd \"$D\" && test -d dbt_vendor/dbt_utils || { echo \"PULSE packaging bug: vendored dbt_vendor/dbt_utils is missing from the staged project; Hub packages are vendored at compile time\" >&2; exit 2; } && python -c \"import multiprocessing as mp, dbt.mp_context as M; M._MP_CONTEXT=mp.get_context('fork'); M.get_mp_context=lambda: M._MP_CONTEXT; from dbt.cli.main import cli; cli()\" build --select tag:msp_loan_master_ingestion,tag:clean_loan_master --target gcp --profiles-dir .",
            env={'PULSE_BUSINESS_DATE': '{{ ds }}', 'PULSE_PROCESSING_TS': '{{ ts }}', 'PULSE_BQ_PROJECT': 'wf-pulse-agentic-dev2', 'PULSE_BQ_LOCATION': 'us-central1', 'PULSE_BQ_DATASET': 'pulse_silver'},
        )

    with TaskGroup('mask_loan_master_pii') as tg_mask_loan_master_pii:
        mask_loan_master_pii = BashOperator(
            task_id='mask_loan_master_pii',
            bash_command="D=$(mktemp -d /tmp/dbt.XXXXXX) && gcloud storage rsync -r gs://pulse-home-lending-dev-files/servicing/pipelines/msp_loan_master_ingestion/dbt_project \"$D\" && cd \"$D\" && test -d dbt_vendor/dbt_utils || { echo \"PULSE packaging bug: vendored dbt_vendor/dbt_utils is missing from the staged project; Hub packages are vendored at compile time\" >&2; exit 2; } && python -c \"import multiprocessing as mp, dbt.mp_context as M; M._MP_CONTEXT=mp.get_context('fork'); M.get_mp_context=lambda: M._MP_CONTEXT; from dbt.cli.main import cli; cli()\" build --select tag:msp_loan_master_ingestion,tag:mask_loan_master_pii --target gcp --profiles-dir .",
            env={'PULSE_BUSINESS_DATE': '{{ ds }}', 'PULSE_PROCESSING_TS': '{{ ts }}', 'PULSE_BQ_PROJECT': 'wf-pulse-agentic-dev2', 'PULSE_BQ_LOCATION': 'us-central1', 'PULSE_BQ_DATASET': 'pulse_silver'},
        )

    with TaskGroup('loan_master_scd2') as tg_loan_master_scd2:
        loan_master_scd2 = BashOperator(
            task_id='loan_master_scd2',
            bash_command="D=$(mktemp -d /tmp/dbt.XXXXXX) && gcloud storage rsync -r gs://pulse-home-lending-dev-files/servicing/pipelines/msp_loan_master_ingestion/dbt_project \"$D\" && cd \"$D\" && test -d dbt_vendor/dbt_utils || { echo \"PULSE packaging bug: vendored dbt_vendor/dbt_utils is missing from the staged project; Hub packages are vendored at compile time\" >&2; exit 2; } && python -c \"import multiprocessing as mp, dbt.mp_context as M; M._MP_CONTEXT=mp.get_context('fork'); M.get_mp_context=lambda: M._MP_CONTEXT; from dbt.cli.main import cli; cli()\" build --select tag:msp_loan_master_ingestion,tag:loan_master_scd2 --target gcp --profiles-dir .",
            env={'PULSE_BUSINESS_DATE': '{{ ds }}', 'PULSE_PROCESSING_TS': '{{ ts }}', 'PULSE_BQ_PROJECT': 'wf-pulse-agentic-dev2', 'PULSE_BQ_LOCATION': 'us-central1', 'PULSE_BQ_DATASET': 'pulse_gold'},
        )

    with TaskGroup('validate_loan_master') as tg_validate_loan_master:
        validate_loan_master = BashOperator(
            task_id='validate_loan_master',
            bash_command="D=$(mktemp -d /tmp/dbt.XXXXXX) && gcloud storage rsync -r gs://pulse-home-lending-dev-files/servicing/pipelines/msp_loan_master_ingestion/dbt_project \"$D\" && cd \"$D\" && test -d dbt_vendor/dbt_utils || { echo \"PULSE packaging bug: vendored dbt_vendor/dbt_utils is missing from the staged project; Hub packages are vendored at compile time\" >&2; exit 2; } && python -c \"import multiprocessing as mp, dbt.mp_context as M; M._MP_CONTEXT=mp.get_context('fork'); M.get_mp_context=lambda: M._MP_CONTEXT; from dbt.cli.main import cli; cli()\" build --select tag:msp_loan_master_ingestion,tag:validate_loan_master --target gcp --profiles-dir .",
            env={'PULSE_BUSINESS_DATE': '{{ ds }}', 'PULSE_PROCESSING_TS': '{{ ts }}', 'PULSE_BQ_PROJECT': 'wf-pulse-agentic-dev2', 'PULSE_BQ_LOCATION': 'us-central1', 'PULSE_BQ_DATASET': 'pulse_silver'},
        )

    with TaskGroup('advance_loan_master_date') as tg_advance_loan_master_date:
        # Codegen engine: CodegenOpEngine
        # DAG-only blueprint: AdvanceTimeDimension
        # AdvanceTimeDimension 'advance_loan_master_date': NOT IMPLEMENTED - no time-state advance was performed. See issue #118.
        advance_loan_master_date = PythonOperator(
            task_id='advance_loan_master_date',
            python_callable=pulse_advance_time_not_implemented,
            do_xcom_push=False,
        )

    gx_bronze_silver_gate = PythonOperator(
        task_id='gx_bronze_silver_gate',
        python_callable=lambda **ctx: None,
        do_xcom_push=False,
    )
    gx_silver_gold_gate = PythonOperator(
        task_id='gx_silver_gold_gate',
        python_callable=lambda **ctx: None,
        do_xcom_push=False,
    )
    # Intra-layer task group dependencies (from port wirings)
    tg_clean_loan_master >> tg_mask_loan_master_pii
    tg_loan_master_scd2 >> tg_validate_loan_master
    tg_validate_loan_master >> tg_advance_loan_master_date
    tg_ingest_msp_loan_master >> gx_bronze_silver_gate
    gx_bronze_silver_gate >> tg_clean_loan_master
    gx_bronze_silver_gate >> tg_mask_loan_master_pii
    tg_clean_loan_master >> gx_silver_gold_gate
    tg_mask_loan_master_pii >> gx_silver_gold_gate
    gx_silver_gold_gate >> tg_loan_master_scd2
    gx_silver_gold_gate >> tg_validate_loan_master
