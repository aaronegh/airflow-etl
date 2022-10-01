import glob
from datetime import datetime
from airflow import DAG
from operators.ingestion import StagingIngestOperator
from operators.staging_loader import StagingDBLoadOperator
from operators.schema_validator import SchemaValidatorOperator


default_args = {"owner": "airflow", "start_date": datetime(2021, 1, 1)}

with DAG(
    'customer_staging',
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:

    t1 = StagingIngestOperator(
        task_id="customer_staging_ingestion", config_path="/usr/local/airflow/dags/config/customer_staging.yaml"
    )

    t2 = SchemaValidatorOperator(
        task_id="schema_validator",
        config_path="/usr/local/airflow/dags/config/customer_staging.yaml",
    )

    t3 = StagingDBLoadOperator(
        task_id="copy_to_postgres_customer_staging",
        config_path="/usr/local/airflow/dags/config/customer_staging.yaml",
    )

t1 >> t2 >> t3
