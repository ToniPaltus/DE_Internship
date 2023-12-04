import os
from datetime import datetime

import pandas as pd
from airflow.decorators import dag, task, task_group

from config_for_snowflake.config import (account, database, full_file_to_load,
                                         password, schema, streams_query_path,
                                         tables_query_path, user, warehouse)
from sql_query import SnowWorker

# create the worker with Snowflake
worker = SnowWorker(
    user=user,
    password=password,
    account=account,
    warehouse=warehouse,
    database=database,
    schema=schema,
)

default_args = {"owner": "toni_paltus"}


@dag(
    default_args=default_args,
    dag_id="Snowflake_task",
    start_date=datetime.utcnow(),
    schedule=None,
    catchup=False,
)
def task_flow():
    """
    Describe task_flow:
    - prepare db:
        - create tables
        - create streams
    - read csv file
    - load into db:
        - load data from pandas to raw
        - load data from raw to stage
        - load data from stage to master
    :return: None
    """

    @task(task_id="read_csv")
    def read_csv(file_path: str) -> pd.DataFrame:
        """
        Describe read_csv:
        - read the csv file with Pandas
        :param file_path: path to file.csv
        :return: data
        """

        data = pd.read_csv(file_path, index_col=0)
        data.columns = data.columns.str.upper()
        return data

    @task_group(group_id="prepare_db")
    def prepare_db_group():
        """
        Describe prepare_db_group:
        - create tables
        - create streams
        :return: None
        """

        @task(task_id="create_tables")
        def create_tables() -> None:
            """
            Describe create_tables:
            - Use the directory with queries
            - Execute these queries
            :return: None
            """

            full_paths = [
                os.path.join(tables_query_path, file)
                for file in os.listdir(tables_query_path)
            ]
            for query_path in full_paths:
                with open(query_path) as file:
                    worker.execute_query(file.read())

        @task(task_id="create_streams")
        def create_streams() -> None:
            """
            Describe create_streams:
            - Use the directory with queries
            - Execute these queries
            :return: None
            """
            full_paths = [
                os.path.join(streams_query_path, file)
                for file in os.listdir(streams_query_path)
            ]
            for query_path in full_paths:
                with open(query_path) as file:
                    worker.execute_query(file.read())

        # task order
        create_tables() >> create_streams()

    @task_group(group_id="load_into_db")
    def load_into_db_group(data: pd.DataFrame):
        """
        Describe load_into_db_group:
        - load data from pandas to raw
        - load data from raw to stage
        - load data from stage to master
        :param data: result of read_csv
        :return: None
        """

        @task(task_id="load_pandas_to_raw")
        def load_data_from_pandas_to_raw_table(
            data: pd.DataFrame, table_name: str
        ) -> None:
            """
            Write pandas to raw table via Snowflake
            :param data: result of read_csv
            :param table_name:
            :return: None
            """

            worker.write_pandas(data, table_name)

        @task(task_id="load_raw_to_stage")
        def load_data_from_raw_to_stage() -> None:
            """
            Move data from raw_table to stage_table
            :return: None
            """
            worker.execute_query(
                "INSERT INTO STAGE_TABLE SELECT * FROM RAW_TABLE"
            )

        @task(task_id="load_stage_to_master")
        def load_data_from_stage_to_master() -> None:
            """
             Move data from stage_table to master_table
            :return: None
            """

            worker.execute_query(
                "INSERT INTO MASTER_TABLE SELECT * FROM STAGE_TABLE"
            )

        # task order
        load_data_from_raw_to_stage() >> load_data_from_stage_to_master()

    # group order
    prepare_db_group() >> load_into_db_group(read_csv(full_file_to_load))

    # Load all file from simple batches

    # full_paths_to_load = [os.path.join(data_to_load_path, file) \
    # for file in os.listdir(data_to_load_path)]
    # for path in full_paths:
    #     data = read_csv(file_path=path)
    #     load_data_from_pandas_to_raw_table(data, 'RAW_TABLE')
    # load_into_db_group()


task_flow()
