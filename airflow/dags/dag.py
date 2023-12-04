import os
from datetime import datetime
import pandas as pd
import pymongo
from dotenv import load_dotenv, dotenv_values
from airflow.decorators import dag, task, task_group
from airflow.sensors.filesystem import FileSensor

default_args = {
    "owner": "toni",
}

load_dotenv()
creds = dotenv_values(".env")

file_path = os.getenv('FILE_PATH')
mongo_client = os.getenv('MONGO_CLIENT')
db_name = os.getenv('DB_NAME')
collection_name = os.getenv('COLLECTION_NAME')

# DAG
@dag(
    default_args=default_args,
    dag_id="dag",
    schedule=None,
    start_date=datetime.utcnow(),
    catchup=False,
)
def data_flow():
    """
    Describe data flow:
    - Check existing of csv file.
    - Load the csv file.
    - Some data preparation.
    - Load data to local mongodb.
    """

    @task(task_id="read_csv")
    def read_csv(file_path: str) -> pd.DataFrame:
        """
        Task to load the csv to pd.DataFrame.
        :param file_path: path of csv file.
        :return: pd.DataFrame
        """
        return pd.read_csv(file_path)

    @task(task_id="load_to_mongo")
    def load_to_mongo(data: pd.DataFrame) -> pd.DataFrame:
        """
        Task to load pd.DataFrame to mongoDB.
        :param data: prepared dataframe.
        :return: None.
        """

        client = pymongo.MongoClient(mongo_client)
        db = client[db_name]
        collection = db[collection_name]
        collection.drop()
        collection.insert_many(data.to_dict(orient="records"))

    @task_group(group_id="process_data")
    def process_data(data: pd.DataFrame) -> pd.DataFrame:
        """
        Task group to process data.
        :param data: loaded dataframe.
        :return: processed dataframe.
        """

        @task(task_id="data_clean")
        def data_clean(data: pd.DataFrame) -> pd.DataFrame:
            """
            Task to clear data.
            :param data: dataframe.
            :return: dataframe without duplicates and nulls.
            """

            data.drop_duplicates(inplace=True)
            data.dropna(how="all", inplace=True)
            return data

        @task(task_id="replace_nulls")
        def replace_nulls(data: pd.DataFrame) -> pd.DataFrame:
            """
            Task to replace nulls to '-'.
            :param data: dataframe.
            :return: modified dataframe.
            """

            data.fillna(value="-", inplace=True)
            return data

        @task(task_id="sort_by_date")
        def sort_by_date(data: pd.DataFrame) -> pd.DataFrame:
            """
            Task to sort dataframe by data.
            :param data: dataframe.
            :return: sorted dataframe.
            """

            data.sort_values(by="at", inplace=True)
            return data

        @task(task_id="deleting_symbols")
        def delete_symbols(data: pd.DataFrame) -> pd.DataFrame:
            """
            Task to delete simbols.
            :param data: dataframe.
            :return: clear dataframe.
            """

            data["content"].replace(
                to_replace=r"[^\s\w?!.,:;\'\(\)\[\]\{\}\/-]",
                value="",
                regex=True,
                inplace=True,
            )
            return data

        return delete_symbols(sort_by_date(replace_nulls(data_clean(data))))

    # Sensor to check the file
    check_file = FileSensor(task_id="check_file", filepath=file_path)
    loaded_csv = check_file >> read_csv(file_path)

    load_to_mongo(process_data(loaded_csv))

data_flow()