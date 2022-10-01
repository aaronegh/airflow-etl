import csv
import glob
import logging
import yaml
import pandas as pd
from pandas import DataFrame
from datetime import datetime
from pathlib import Path
from airflow.models.baseoperator import BaseOperator


class StagingIngestOperator(BaseOperator):
    """
    This class is used to ingest and transform source data into a target data set in specified format.
    : output format : csv
    """

    def __init__(self, config_path: str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.config_path = config_path

    def __read_yaml_jobfiles__(self, path: str) -> dict:
        with open(path, "r") as f:
            return yaml.safe_load(f)

    # ingest various type of source data
    def __data_ingestion__(self, source: str, type: str) -> DataFrame:
        if type == "jsonlines":
            return pd.read_json(source, orient="records", lines=True)

    # extract domain from email
    def __extract_domain_from_email__(
        self, df: DataFrame, input_col: str, output_col: str
    ) -> DataFrame:
        df[output_col] = df[input_col].str.split("@").str[1]
        return df

    # drop list of columns
    def __drop_column__(self, df: DataFrame, drop_cols: list) -> DataFrame:
        for col in drop_cols:
            df.drop(col, axis=1, inplace=True)
        return df

    # remove duplicates from entire dataframe
    def __remove_duplicates__(self, df: DataFrame) -> DataFrame:
        return df.drop_duplicates()

    # checks for null based input column, useful for non nullable columns
    def __check_column_null__(self, df: DataFrame, null_input_col: str) -> bool:
        return df[null_input_col].isnull().values.any()

    # checks for duplicate on the entire dataframe
    def __check_duplicate__(self, df: DataFrame) -> bool:
        return df.duplicated().any()

    # loads df to csv
    def __load_to_csv__(self, df: DataFrame, path: str, target: str,source_name: str) -> csv:
        logging.info("creating csv")
        for column in df.columns:
            if df[column].dtype == "float64":
                df[column] = (
                    df[column].apply(lambda f: format(f, ".0f")).replace("nan", "")
                )
            df[column] = df[column].fillna("").astype("str")
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        (
            df["IS_CURRENT"],
            df["ETL_DATETIME"],
            df["CREATED_BY"],
            df["DEACTIVATED_DATE"],
        ) = ("Y", timestamp, source_name, "")
        logging.info("done")
        return df.to_csv(f"{path}/{target}.csv", index=False, encoding="utf-8")

    def execute(self, context) -> None:
        config = self.__read_yaml_jobfiles__(self.config_path)
        source = config["source"]
        source_files = glob.glob(f"{source}*")
        source_files = [x for x in source_files if "processed_by_staging" not in x]
        logging.info(f"processing source files :- {source_files}")
        for data in source_files:
            logging.info(data)
            df = self.__data_ingestion__(source=data, type=config["type"])
            # transformation functions
            for transformation in config["transformation"]:
                if transformation["type"] == "extract_domain_from_email":
                    df = self.__extract_domain_from_email__(
                        df=df,
                        input_col=transformation["input_column"],
                        output_col=transformation["output_column"],
                    )
                if transformation["type"] == "drop_column":
                    df = self.__drop_column__(
                        df=df, drop_cols=transformation["drop_columns"]
                    )
                if transformation["type"] == "remove_duplicates":
                    df = self.__remove_duplicates__(df=df)

            # data quality functions
            for dq in config["data_quality"]:
                if dq["type"] == "check_column_null":
                    check_null = self.__check_column_null__(
                        df=df, null_input_col=dq["input_column"]
                    )
                    logging.error(
                        "Null row detected in dataframe."
                    ) if check_null == True else logging.info(
                        "No nulls detected, all good."
                    )
                if dq["type"] == "check_duplicate":
                    check_duplicates = self.__check_duplicate__(df=df)
                    logging.error(
                        "Duplicates found in dataframe."
                    ) if check_duplicates == True else logging.info(
                        "No duplicates found, all good."
                    )

            # output to path
            p = Path(data)
            domain = config["target"]
            source_name = p.stem
            target = f"{domain}_{p.stem}"
            self.__load_to_csv__(df=df, path="./staging", target=target,source_name=source_name)

            # mark file as processed
            p.rename(Path(p.parent, f"{p.stem}_processed_by_staging{p.suffix}"))
