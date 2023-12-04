import logging
import sys

from config.config import db_name, host, password, user
from classes.DBWorker import DBWorker
from functions.logger import init_logger
from functions.parse_args import pasre_args

init_logger("main")
logger = logging.getLogger("main")


def main():
    arguments = pasre_args(sys.argv[1:0])

    students_path = arguments["students_path"]
    rooms_path = arguments["rooms_path"]
    store_format = arguments["store_format"]

    connection = DBWorker(host, user, password, db_name, store_format=store_format)

    # create tables
    create_rooms_query = DBWorker.get_query_from_file("pg_queries/create_rooms.sql")
    connection.execute_query(create_rooms_query)

    create_students_query = DBWorker.get_query_from_file(
        "pg_queries/create_students.sql"
    )
    connection.execute_query(create_students_query)

    # create copies (execute once!)

    copy_rooms_query = \
    f"""COPY rooms FROM '{rooms_path}' DELIMITER ',' CSV HEADER;"""
    copy_students_query = \
    f"""COPY students FROM '{students_path}' DELIMITER ',' CSV HEADER;"""

    connection.execute_query(copy_rooms_query)
    connection.execute_query(copy_students_query)

    query = DBWorker.get_query_from_file("pg_queries/query1.sql")
    connection.execute_query(query, show=True)


# python main.py -students='/home/tonipaltus/Innowise/python_intro/data/students.csv' -rooms='/home/tonipaltus/Innowise/python_intro/data/rooms.csv' -store_format='JJJJ'

if __name__ == "__main__":
    logger.info("Starting service...")
    main()
    logger.info("Stopping service...")
