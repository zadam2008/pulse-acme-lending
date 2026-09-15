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
    description='Ingests daily loan master extracts from MSP, cleans, masks PII, tracks historical changes via SCD2, and validates before publishing to gold.',
    default_args=default_args,
    schedule='0 6 * * 1-5',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    # PULSE re-run contract: IDEMPOTENT_OVERWRITE (source: PLATFORM_DEFAULT)
    max_active_runs=2,
    tags=['pulse', 'tenant-home-lending', 'servicing'],
) as dag:

    with TaskGroup('ingestloanmaster') as tg_ingestloanmaster:
        ingestloanmaster = DataprocCreateBatchOperator(
            task_id='ingestloanmaster',
            project_id='wf-pulse-agentic-dev2',
            region='us-central1',
            batch={
                'pyspark_batch': {'main_python_file_uri': 'gs://pulse-home-lending-dev-files/servicing/pipelines/msp_loan_master_ingestion/jobs/ingestion/ingestloanmaster_ingest.py'},
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

    with TaskGroup('cleanloanmaster') as tg_cleanloanmaster:
        cleanloanmaster = BashOperator(
            task_id='cleanloanmaster',
            bash_command="D=$(mktemp -d /tmp/dbt.XXXXXX) && gcloud storage rsync -r gs://pulse-home-lending-dev-files/servicing/pipelines/msp_loan_master_ingestion/dbt_project \"$D\" && cd \"$D\" && test -d dbt_vendor/dbt_utils || { echo \"PULSE packaging bug: vendored dbt_vendor/dbt_utils is missing from the staged project; Hub packages are vendored at compile time\" >&2; exit 2; } && python -c \"import multiprocessing as mp, dbt.mp_context as M; M._MP_CONTEXT=mp.get_context('fork'); M.get_mp_context=lambda: M._MP_CONTEXT; from dbt.cli.main import cli; cli()\" build --select tag:msp_loan_master_ingestion,tag:cleanloanmaster --target gcp --profiles-dir .",
            env={'PULSE_BUSINESS_DATE': '{{ ds }}', 'PULSE_PROCESSING_TS': '{{ ts }}', 'PULSE_BQ_PROJECT': 'wf-pulse-agentic-dev2', 'PULSE_BQ_LOCATION': 'us-central1', 'PULSE_BQ_DATASET': 'pulse_silver'},
        )

    with TaskGroup('maskloanmasterpii') as tg_maskloanmasterpii:
        maskloanmasterpii = BashOperator(
            task_id='maskloanmasterpii',
            bash_command="D=$(mktemp -d /tmp/dbt.XXXXXX) && gcloud storage rsync -r gs://pulse-home-lending-dev-files/servicing/pipelines/msp_loan_master_ingestion/dbt_project \"$D\" && cd \"$D\" && test -d dbt_vendor/dbt_utils || { echo \"PULSE packaging bug: vendored dbt_vendor/dbt_utils is missing from the staged project; Hub packages are vendored at compile time\" >&2; exit 2; } && python -c \"import multiprocessing as mp, dbt.mp_context as M; M._MP_CONTEXT=mp.get_context('fork'); M.get_mp_context=lambda: M._MP_CONTEXT; from dbt.cli.main import cli; cli()\" build --select tag:msp_loan_master_ingestion,tag:maskloanmasterpii --target gcp --profiles-dir .",
            env={'PULSE_BUSINESS_DATE': '{{ ds }}', 'PULSE_PROCESSING_TS': '{{ ts }}', 'PULSE_BQ_PROJECT': 'wf-pulse-agentic-dev2', 'PULSE_BQ_LOCATION': 'us-central1', 'PULSE_BQ_DATASET': 'pulse_silver'},
        )

    with TaskGroup('loanmasterscd2') as tg_loanmasterscd2:
        loanmasterscd2 = BashOperator(
            task_id='loanmasterscd2',
            bash_command="D=$(mktemp -d /tmp/dbt.XXXXXX) && gcloud storage rsync -r gs://pulse-home-lending-dev-files/servicing/pipelines/msp_loan_master_ingestion/dbt_project \"$D\" && cd \"$D\" && test -d dbt_vendor/dbt_utils || { echo \"PULSE packaging bug: vendored dbt_vendor/dbt_utils is missing from the staged project; Hub packages are vendored at compile time\" >&2; exit 2; } && python -c \"import multiprocessing as mp, dbt.mp_context as M; M._MP_CONTEXT=mp.get_context('fork'); M.get_mp_context=lambda: M._MP_CONTEXT; from dbt.cli.main import cli; cli()\" build --select tag:msp_loan_master_ingestion,tag:loanmasterscd2 --target gcp --profiles-dir .",
            env={'PULSE_BUSINESS_DATE': '{{ ds }}', 'PULSE_PROCESSING_TS': '{{ ts }}', 'PULSE_BQ_PROJECT': 'wf-pulse-agentic-dev2', 'PULSE_BQ_LOCATION': 'us-central1', 'PULSE_BQ_DATASET': 'pulse_gold'},
        )

    with TaskGroup('validateloanmaster') as tg_validateloanmaster:
        validateloanmaster = BashOperator(
            task_id='validateloanmaster',
            bash_command="D=$(mktemp -d /tmp/dbt.XXXXXX) && gcloud storage rsync -r gs://pulse-home-lending-dev-files/servicing/pipelines/msp_loan_master_ingestion/dbt_project \"$D\" && cd \"$D\" && test -d dbt_vendor/dbt_utils || { echo \"PULSE packaging bug: vendored dbt_vendor/dbt_utils is missing from the staged project; Hub packages are vendored at compile time\" >&2; exit 2; } && python -c \"import multiprocessing as mp, dbt.mp_context as M; M._MP_CONTEXT=mp.get_context('fork'); M.get_mp_context=lambda: M._MP_CONTEXT; from dbt.cli.main import cli; cli()\" build --select tag:msp_loan_master_ingestion,tag:validateloanmaster --target gcp --profiles-dir .",
            env={'PULSE_BUSINESS_DATE': '{{ ds }}', 'PULSE_PROCESSING_TS': '{{ ts }}', 'PULSE_BQ_PROJECT': 'wf-pulse-agentic-dev2', 'PULSE_BQ_LOCATION': 'us-central1', 'PULSE_BQ_DATASET': 'pulse_silver'},
        )

    with TaskGroup('advanceloanmasterdate') as tg_advanceloanmasterdate:
        # Codegen engine: CodegenOpEngine
        # DAG-only blueprint: AdvanceTimeDimension
        # AdvanceTimeDimension 'advanceloanmasterdate': NOT IMPLEMENTED - no time-state advance was performed. See issue #118.
        advanceloanmasterdate = PythonOperator(
            task_id='advanceloanmasterdate',
            python_callable=pulse_advance_time_not_implemented,
            do_xcom_push=False,
        )

    with TaskGroup('loanmasterschedule') as tg_loanmasterschedule:
        # Codegen engine: CodegenOpEngine
        # DAG-only blueprint: ScheduleAndTriggers
        # schedule_interval='0 6 * * 1-5'
        pass

    with TaskGroup('detectloanmasterschemadrift') as tg_detectloanmasterschemadrift:
        detectloanmasterschemadrift = DataprocCreateBatchOperator(
            task_id='detectloanmasterschemadrift',
            project_id='wf-pulse-agentic-dev2',
            region='us-central1',
            batch={
                'pyspark_batch': {'main_python_file_uri': 'gs://pulse-home-lending-dev-files/servicing/pipelines/msp_loan_master_ingestion/gx/checkpoints/detectloanmasterschemadrift_checkpoint.py'},
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

    with TaskGroup('checkloanmasterfreshness') as tg_checkloanmasterfreshness:
        checkloanmasterfreshness = DataprocCreateBatchOperator(
            task_id='checkloanmasterfreshness',
            project_id='wf-pulse-agentic-dev2',
            region='us-central1',
            batch={
                'pyspark_batch': {'main_python_file_uri': 'gs://pulse-home-lending-dev-files/servicing/pipelines/msp_loan_master_ingestion/gx/checkpoints/checkloanmasterfreshness_checkpoint.py'},
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
    tg_cleanloanmaster >> tg_maskloanmasterpii
    tg_loanmasterscd2 >> tg_validateloanmaster
    tg_validateloanmaster >> tg_advanceloanmasterdate
    tg_cleanloanmaster >> tg_detectloanmasterschemadrift
    tg_cleanloanmaster >> tg_checkloanmasterfreshness
    tg_ingestloanmaster >> gx_bronze_silver_gate
    gx_bronze_silver_gate >> tg_cleanloanmaster
    gx_bronze_silver_gate >> tg_maskloanmasterpii
    gx_bronze_silver_gate >> tg_detectloanmasterschemadrift
    gx_bronze_silver_gate >> tg_checkloanmasterfreshness
    tg_cleanloanmaster >> gx_silver_gold_gate
    tg_maskloanmasterpii >> gx_silver_gold_gate
    tg_detectloanmasterschemadrift >> gx_silver_gold_gate
    tg_checkloanmasterfreshness >> gx_silver_gold_gate
    gx_silver_gold_gate >> tg_loanmasterscd2
    gx_silver_gold_gate >> tg_validateloanmaster
