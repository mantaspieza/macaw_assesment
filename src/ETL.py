# from data_processing import Data_processing
# import pandas as pd
# import pyodbc
import os, uuid
from azure.storage.blob import (
    BlobServiceClient,
    __version__,
)
from dotenv import load_dotenv

from data_processing import Data_processing

load_dotenv()


class ETL:
    """
    Takes yellow taxi dat from local storage (As i have no SSH to take it directly from page),
     uploads raw data to azure blob,
      extracts raw data from azure blob,
       cleans it and uploads cleaned back to blob
    """

    def __init__(self) -> None:
        self.connection = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        self.blob_service_client = BlobServiceClient.from_connection_string(
            self.connection
        )

    def upload_raw_data_to_azure(
        self, start_month: int, end_month: int, year: str = "2021"
    ):
        container_name = "raw-yellow-taxi-data"
        self.blob_service_client.create_container(container_name)
        raw_data_file_path = "./data/raw_data"
        months = [str(i).zfill(2) for i in range(start_month, end_month + 1)]

        for month in months:

            temp_filename = f"yellow_tripdata_{year}-" + str(month) + ".csv"
            blob_client = self.blob_service_client.get_blob_client(
                container=container_name, blob=temp_filename
            )

            # uploads data to azure
            upload_file_path = os.path.join(raw_data_file_path, temp_filename)
            with open(upload_file_path, "rb") as data:
                blob_client.upload_blob(data)

    def extract_single_taxi_blob(
        self, file_name: str, file_content, local_blob_path: str
    ):

        download_file_path = os.path.join(local_blob_path, file_name)

        with open(download_file_path, "wb") as file:
            file.write(file_content)

    def extract_raw_taxi_data_from_azure(self):
        my_container = self.blob_service_client.get_container_client(
            "raw-yellow-taxi-data"
        )
        list_of_blobs = my_container.list_blobs()
        local_blob_path = "../data/extracted_from_azure_raw"
        for blob in list_of_blobs:
            single_csv_file = (
                my_container.get_blob_client(blob).download_blob().readall()
            )
            self.extract_single_taxi_blob(blob.name, single_csv_file, local_blob_path)

    def transform_raw_data(self, start_month: int, end_month: int, year: int):
        data_processing = Data_processing(start_month, end_month, year)
        data_processing.run()

    def upload_transformed_data_to_azure(
        self, start_month: int, end_month: int, year: int
    ):
        container_name = "transformed-yellow-taxi-data"
        try:
            self.blob_service_client.create_container(container_name)
        except:
            self.blob_service_client.get_container_client(container_name)
        raw_data_file_path = "../data/transformed_data"
        months = [str(i).zfill(2) for i in range(start_month, end_month + 1)]

        for month in months:

            temp_filename = (
                "clean_yellow_trip_data_" + str(year) + "-" + str(month) + ".csv"
            )
            blob_client = self.blob_service_client.get_blob_client(
                container=container_name, blob=temp_filename
            )

            # uploads data to azure
            upload_file_path = os.path.join(raw_data_file_path, temp_filename)
            with open(upload_file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)

    def extract_transformed_taxi_data_from_azure(self):
        my_container = self.blob_service_client.get_container_client(
            "transformed-yellow-taxi-data"
        )
        list_of_blobs = my_container.list_blobs()
        local_blob_path = "../data/extracted_from_azure_transformed"
        for blob in list_of_blobs:
            single_csv_file = (
                my_container.get_blob_client(blob).download_blob().readall()
            )
            self.extract_single_taxi_blob(blob.name, single_csv_file, local_blob_path)

    def delete_container(self, container_name: str):
        my_container = self.blob_service_client.get_container_client(
            container=container_name
        )

        my_container.delete_container()

    def delete_blob(self, container_name: str, blob_name: str):
        my_container = self.blob_service_client.get_container_client(
            container=container_name
        )

        my_container.delete_blob(blob_name)
