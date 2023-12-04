import logging

import pandas as pd
import psycopg2

from functions.logger import init_logger

init_logger("database")
logger = logging.getLogger("database")


class DBWorker:
    """It's a class for simple connection to database.
    There are many useful methods to work with db."""

    def __init__(
        self, host: str, user: str, password: str, db_name: str, store_format: str
    ):
        """This method initializes a connection with arguments.
        input:
            host - hostname,
            user - username,
            db_name - database name,
            password - role password,
            store_format - format to store the results of queries.
        return: specimen of class with arguments and connection with database"""

        self.__host = host
        self.__user = user
        self.__password = password
        self.__db_name = db_name
        self.__store_format = store_format.lower()

        self.__connection = self.__get_connect()

    def __del__(self):
        """This method closes the connection to database."""

        logger.debug("Closing connection")
        self.__connection.close()

    @classmethod
    def get_query_from_file(self, file_path: str) -> str:
        """This method creates a query from file and returns a string.
        input: file path - string with path
        return: query commands - string of SQL commands"""

        logger.info('Opening query file')
        file = open(f"{file_path}")
        query = file.read()
        file.close()
        logger.info('Closing query file')

        return query

    def __get_connect(self) -> psycopg2.connect:
        """This method create a connection to database with necessary arguments and returns it.
        input: nothing
        return: connection with database"""

        logger.debug("Opening connection")

        connecton = psycopg2.connect(
            host=self.__host,
            user=self.__user,
            password=self.__password,
            database=self.__db_name,
        )
        connecton.autocommit = True

        return connecton

    def execute_query(self, query: str, show=False):
        """This method executes query and can save the result into chosen store format.
        input: query - a string of sql commands
        return: print into console query result if show=True"""

        with self.__connection.cursor() as cursor:
            logger.info("Executing query...")
            cursor.execute(f"""{query}""")

            result = []
            try:
                result = cursor.fetchall()
            except Exception as ex:
                logger.error(ex)

            if show:
                print(result, type(result))

            if isinstance(result, list):
                if self.__store_format == "json":
                    self.__save_to_json(result)
                elif self.__store_format == "csv":
                    self.__save_to_csv(result)
                else:
                    logger.warning("Store format is no exist...")

    def __save_to_json(self, query_result: list):
        """This method save query result into JSON.
        input: list - query result
        return: void - just save into file"""

        df = pd.DataFrame(query_result)
        df.to_json("saves/query_result.json", orient="records")
        logger.info("Save format json")

    def __save_to_csv(self, query_result: list):
        """This method save query result into CSV.
        input: list - query result
        return: void - just save into file"""

        df = pd.DataFrame(query_result)
        df.to_csv("saves/query_result.csv")
        logger.info("Save format csv")