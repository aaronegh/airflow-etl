import yaml
import logging
import glob
import pandas as pd
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook


class SchemaValidatorOperator(BaseOperator):
    """
    This class is used to validate source and target schema and create new staging tables.
    """

    def __init__(self, config_path: str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.config_path = config_path

    def __read_yaml_jobfiles__(self, path: str) -> dict:
        with open(path, "r") as f:
            return yaml.safe_load(f)

    def execute(self, context) -> None:

        # scans for staging files for processing
        config = self.__read_yaml_jobfiles__(self.config_path)
        staging_files = glob.glob("./staging/*")
        logging.info(staging_files)

        # connect to specified target database
        if config["target_db"] == "postgres":
            pg_hook = PostgresHook(postgres_conn_id=config["target_connection_id"])
            conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # loads staging file into target database sequentially
        for file in staging_files:
            df = pd.read_csv(file)
            columns = df.dtypes.apply(lambda x: x.name).to_dict()

            # formatting datatypes of columns suitable for sql DBs
            for column, datatype in columns.items():
                if column in ["ETL_DATETIME", "DEACTIVATED_DATE"]:
                    columns[column] = "timestamptz"
                elif datatype == "object":
                    columns[column] = "VARCHAR(1000)"
                elif datatype in ["float64", "int64"]:
                    columns[column] = "bigint"

            # preparing variables for SQL statements
            columns_string = ", ".join(
                f"{column} {datatype}" for column, datatype in columns.items()
            )

            # execution of source and target schema validation process
            # creates target table if not exist -> add new column into target table if not exist
            create_table_statement = """
            CREATE TABLE IF NOT EXISTS staging.{} ({});
            """.format(
                config["target"], columns_string
            )
            cursor.execute(create_table_statement)
            cursor.execute("COMMIT")

            for column, datatype in columns.items():
                alter_table_statement = """
                ALTER TABLE staging.{} ADD COLUMN IF NOT EXISTS {};
                """.format(
                    config["target"], f"{column} {datatype}"
                )
                cursor.execute(alter_table_statement)
                cursor.execute("COMMIT")
