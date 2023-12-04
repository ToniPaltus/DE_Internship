import pandas as pd
from snowflake.connector import SnowflakeConnection
from snowflake.connector.pandas_tools import write_pandas


class SnowWorker:
    """
    Class for work with Snowflake:
    - includes all necessary connection parameters
    - includes methods for work
    """

    def __init__(self, user, password, account, warehouse, database, schema):
        """Initialize a connection"""
        self.__user = user
        self.__password = password
        self.__account = account
        self.__warehouse = warehouse
        self.__database = database
        self.__schema = schema

        self.__connection = SnowflakeConnection(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema,
        )

    def __del__(self):
        self.__connection.close()

    @classmethod
    def get_query_from_file(cls, file_path: str) -> str:
        """
        Get query as a text
        :param file_path: path to query
        :return: query
        """
        with open(f"{file_path}") as file:
            query = file.read()
        return query

    def execute_query(self, query: str) -> None:
        """
        Execute query to db
        :param query: query to execute
        :return: None
        """
        self.__connection.cursor().execute(query)

    def write_pandas(self, data: pd.DataFrame, table_name: str) -> None:
        """
        Write from pandas to raw
        :param data:
        :param table_name: to write
        :return: None
        """
        write_pandas(self.__connection, data, table_name)
