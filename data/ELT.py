# from data_processing import Data_processing
# import pandas as pd
# import pyodbc
import os, uuid
from azure.storage.blob import (
    BlobServiceClient,
    BlobClient,
    ContainerClient,
    __version__,
    upload_blob_to_url,
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
        raw_data_file_path = "raw_data"
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

    def extract_single_taxi_blob(self, file_name: str, file_content):
        local_blob_path = "extracted_from_azure"
        download_file_path = os.path.join(local_blob_path, file_name)

        with open(download_file_path, "wb") as file:
            file.write(file_content)

    def extract_raw_taxi_data_from_azure(self):
        my_container = self.blob_service_client.get_container_client(
            "raw-yellow-taxi-data"
        )
        list_of_blobs = my_container.list_blobs()

        for blob in list_of_blobs:
            single_csv_file = (
                my_container.get_blob_client(blob).download_blob().readall()
            )
            self.extract_single_taxi_blob(blob.name, single_csv_file)

    def transform_raw_data(self, start_month: int, end_month: int, year: int):
        data_processing = Data_processing(start_month, end_month, year)
        data_processing.run()

    def upload_transformed_data_to_azure(
        self, start_month: int, end_month: int, year: str = "2021"
    ):
        container_name = "transformed-raw-yellow-taxi-data"
        self.blob_service_client.create_container(container_name)
        raw_data_file_path = "transformed_data"
        months = [str(i).zfill(2) for i in range(start_month, end_month + 1)]

        for month in months:

            temp_filename = f"clean_yellow_tripdata_{year}-" + str(month) + ".csv"
            blob_client = self.blob_service_client.get_blob_client(
                container=container_name, blob=temp_filename
            )

            # uploads data to azure
            upload_file_path = os.path.join(raw_data_file_path, temp_filename)
            with open(upload_file_path, "rb") as data:
                blob_client.upload_blob(data)


test = ETL()
# test.upload_raw_data_to_azure(1,7)
test.transform_raw_data(1, 2, 2021)
