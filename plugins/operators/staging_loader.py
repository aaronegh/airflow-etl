import os
import yaml
import logging
import glob
import pandas as pd
from pathlib import Path
from datetime import datetime
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook


class StagingDBLoadOperator(BaseOperator):
    """
    This class is used to upload csv in a blob to a staging database.
    : output format : csv
    """

    def __init__(self, config_path: str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.config_path = config_path

    def __read_yaml_jobfiles__(self, path: str) -> dict:
        with open(path, "r") as f:
            return yaml.safe_load(f)

    def execute(self, context) -> None:
        config = self.__read_yaml_jobfiles__(self.config_path)
        staging_files = glob.glob("./staging/*")
        staging_files = [x for x in staging_files if "processed_by_staging" not in x]
        logging.info(staging_files)
        staging_files.sort()
        logging.info(staging_files)
        if config["target_db"] == "postgres":
            pg_hook = PostgresHook(postgres_conn_id=config["target_connection_id"])
            conn = pg_hook.get_conn()
        cursor = conn.cursor()
        for file in staging_files:
            df = pd.read_csv(file)
            columns = df.dtypes.apply(lambda x: x.name).to_dict()
            for column, datatype in columns.items():
                if column in ["ETL_DATETIME", "DEACTIVATED_DATE"]:
                    columns[column] = "timestamptz"
                elif datatype == "object":
                    columns[column] = "VARCHAR(1000)"
                elif datatype in ["float64", "int64"]:
                    columns[column] = "bigint"
            columns_string = ", ".join(
                f"{column} {datatype}" for column, datatype in columns.items()
            )
            cursor.execute(
                """
            DROP TABLE IF EXISTS staging.tmp_{};
            """.format(
                    config["target"]
                )
            )
            cursor.execute("COMMIT")
            create_table_statement = """
            CREATE TABLE IF NOT EXISTS staging.tmp_{} ({});
            """.format(
                config["target"], columns_string
            )
            cursor.execute(create_table_statement)
            cursor.execute("COMMIT")
            abs_path = os.path.abspath(file)
            copy_table_statement = """
            COPY staging.tmp_{}
            FROM '{}'
            DELIMITER ','
            CSV HEADER;
            """.format(
                config["target"], abs_path
            )
            cursor.execute(copy_table_statement)
            cursor.execute("COMMIT")
            remove_scd_columns = columns.copy()
            for i in ["IS_CURRENT", "ETL_DATETIME", "CREATED_BY", "DEACTIVATED_DATE"]:
                del remove_scd_columns[i]
            columns_only_string = ", ".join(
                f"{column}" for column, datatype in columns.items()
            )
            join_condition = (
                f'TARGET.{config["key_column"]} = SOURCE.{config["key_column"]} '
            )
            insert_columns = columns_only_string
            value_columns = ", ".join(
                f"SOURCE.{column}" for column, datatype in columns.items()
            )
            match_condition = "OR ".join(
                f"TARGET.{column} <> SOURCE.{column} "
                for column, datatype in remove_scd_columns.items()
            )
            set_parameters = """
            is_current = 'N',
            deactivated_date = '{}'
            """.format(
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            )
            new_insert_table_statement = """
            INSERT INTO staging.{} ({}) SELECT {} FROM staging.tmp_{} SOURCE left join staging.{} TARGET on {} WHERE TARGET.{} IS NULL
            """.format(
                config["target"],
                columns_only_string,
                value_columns,
                config["target"],
                config["target"],
                join_condition,
                config["key_column"],
            )
            cursor.execute(new_insert_table_statement)
            cursor.execute("COMMIT")
            insert_table_statement = """
            INSERT INTO staging.{} ({}) SELECT {} FROM staging.tmp_{} SOURCE left join staging.{} TARGET on {} WHERE ({}) and TARGET.deactivated_date is NULL AND TARGET.created_by < SOURCE.created_by;
            """.format(
                config["target"],
                columns_only_string,
                value_columns,
                config["target"],
                config["target"],
                join_condition,
                match_condition,
            )
            logging.info(insert_table_statement)
            cursor.execute(insert_table_statement)
            cursor.execute("COMMIT")
            update_table_statement = """
            UPDATE staging.{} 
            SET {} 
            from ( SELECT {}, CREATED_BY , ROW_NUMBER() OVER(PARTITION BY {}, IS_CURRENT ORDER BY CREATED_BY DESC) RN
            FROM staging.{}) as TMP
            where TMP.RN > 1 AND STAGING.{}.{} = TMP.{} AND STAGING.{}.CREATED_BY = TMP.CREATED_BY;
            """.format(
                config["target"],
                set_parameters,
                config["key_column"],
                config["key_column"],
                config["target"],
                config["target"],
                config["key_column"],
                config["key_column"],
                config["target"],
            )
            cursor.execute(update_table_statement)
            cursor.execute("COMMIT")

            # mark file as processed
            p = Path(file)
            p.rename(Path(p.parent, f"{p.stem}_processed_by_staging{p.suffix}"))
