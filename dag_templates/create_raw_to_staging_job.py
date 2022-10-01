import glob
from datetime import datetime
from airflow import DAG
from operators.ingestion import StagingIngestOperator
from operators.staging_loader import StagingDBLoadOperator
from operators.schema_validator import SchemaValidatorOperator


default_args = {"owner": "airflow", "start_date": datetime(2021, 1, 1)}

with DAG(
    input_dag_id,
    schedule_interval=input_schedule_interval,
    default_args=default_args,
    catchup=False,
) as dag:

    t1 = StagingIngestOperator(
        task_id="input_job_name_ingestion", config_path="input_config_path"
    )

    t2 = SchemaValidatorOperator(
        task_id="schema_validator",
        config_path="input_config_path",
    )

    t3 = StagingDBLoadOperator(
        task_id="copy_to_postgres_input_job_name",
        config_path="input_config_path",
    )

t1 >> t2 >> t3
