import logging
import os
from azure.storage.blob import (
    BlobServiceClient,
    __version__,
)
from dotenv import load_dotenv
from src.data_processing import Data_processing

load_dotenv()


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    "%(asctime)s:%(levelname)s:%(name)s:%(funcName)s:%(message)s"
)

file_handler = logging.FileHandler("logs/ETL.log", "w")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)


class ETL:
    """
    Takes raw data from raw_data folder, applies cleans and applies transformations and saves it to transformed_data folder.

    ::Parameters::
    :: None

    :: IMPORTANT::
    : It is required to have access to azure servers.
    :: Requirements ::
    :Azure_storage_connection_string :

    :: Functions ::
    :: delete_container         : -> deletes container with predefined name in function
    :: upload_raw_data_to_azure : -> stores raw taxi data from raw_data folder to azure blob storage
    :: extract_singe_blob       : -> extracts single blob from azure blob storage (used for batch extractions)
    :: extract_raw_taxi_data_from_azure: -> extracts all raw taxi data from azure
    :: transform_raw_data       : -> transforms raw data using Data_processing class
    :: upload_transformed_data_to_azure: -> uploads transformed data to azure blob storage
    :: extract_transformed_data_from_azure: -> extracts all transformed data from azure blob storage
    :: delete blob              : -> deletes blob from azure blob storage


    return:: None

    """

    def __init__(self) -> None:
        self.connection = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        self.blob_service_client = BlobServiceClient.from_connection_string(
            self.connection
        )

    def delete_container(self, container_name: str):
        """
        Deletes container from azure blob storage.

        param: container_name:-> name of the container in azure blobs storage

        return:: None
        """
        try:
            my_container = self.blob_service_client.get_container_client(
                container=container_name
            )
            logger.debug(f"successfully connected to {container_name}")
            my_container.delete_container()
            logger.debug("container successfully deleted")
        except ConnectionError or ValueError:
            logger.exception(
                "the container {container_name} was NOT deleted. check the code."
            )
        else:
            logger.info("Container {container_name} was successfuly deleted")

    def upload_raw_data_to_azure(
        self, start_month: int, end_month: int, year: str = "2021"
    ):
        """
        Uploads raw data from local raw_data folder to azure blob storage.

        param: start_month:-> first month of dataset (tipically 1)
        param: end_month:-> last month of dataset (tipically 12) in this case 7
        param: year:-> str value set to 2021

        return:: None
        """
        try:
            container_name = "raw-yellow-taxi-data"
            try:
                self.blob_service_client.create_container(container_name)
                logger.debug("container created successfully")

            except:
                logger.info("Table was already created!")
            else:
                self.blob_service_client.get_container_client(container_name)
                logger.info("container existed, client will be retrieved")

            raw_data_file_path = "data/raw_data"
            months = [str(i).zfill(2) for i in range(start_month, end_month + 1)]

            for month in months:

                temp_filename = f"yellow_tripdata_{year}-" + str(month) + ".csv"
                blob_client = self.blob_service_client.get_blob_client(
                    container=container_name, blob=temp_filename
                )
                logger.debug("blob client created")

                # uploads data to azure
                upload_file_path = os.path.join(raw_data_file_path, temp_filename)
                with open(upload_file_path, "rb") as data:
                    blob_client.upload_blob(data)

                logger.debug(
                    f"master, problem uploading {temp_filename} to container {container_name}"
                )
                logger.info(
                    f"{temp_filename} was succesfully moved to {container_name}"
                )

        except ConnectionRefusedError:
            logger.exception("there was a problem with connection or path provided")
        else:
            # removes files after upload is compleated.
            dir = "data/raw_data"
            for file in os.listdir(dir):
                os.remove(os.path.join(dir, file))
            logger.info(
                "Raw data was successfully transfered and deleted from data/raw folder."
            )

    def extract_single_taxi_blob(
        self, file_name: str, file_content, local_blob_path: str
    ):
        """
        writes single blob from azure blob storage. used in other functions for bulk extraction.

        :param file_name:-> title of the csv file to retreive.
        :param local_blob_path:-> path to the blob on the device.

        return:: None
        """
        try:
            download_file_path = os.path.join(local_blob_path, file_name)

            with open(download_file_path, "wb") as file:
                file.write(file_content)
            logger.debug("There was a problem extracting single taxi blob")
        except:
            logger.exception(
                f"there was a problem reaching single {file_name} at {local_blob_path}"
            )

    def extract_raw_taxi_data_from_azure(self):
        """
        Extracts all raw taxi data from azure blob storage.

        :param: None
        return: None
        """
        try:
            my_container = self.blob_service_client.get_container_client(
                "raw-yellow-taxi-data"
            )
            list_of_blobs = my_container.list_blobs()
            local_blob_path = "data/extracted_from_azure_raw"

            for blob in list_of_blobs:
                single_csv_file = (
                    my_container.get_blob_client(blob).download_blob().readall()
                )
                self.extract_single_taxi_blob(
                    blob.name, single_csv_file, local_blob_path
                )

            logger.info(f"blob was successfuly extracted")
        except ConnectionError:
            logger.exception(
                "there was an error while extracting raw taxi data from azure, master"
            )
        else:
            logger.info(
                "ALL files were SUCCESSFULLY moved from blob storage -> machine"
            )

    def transform_raw_data(self, start_month: int, end_month: int, year: int):
        """
        Transforms data using Data_processing class.

        :param start_month:-> first month (int 1/2/3 etc.) to start transformations.
        :param end_month:-> last month (int (1/2/3/ etc)) to end transformations.
        :param year:-> year (int) of datasets to be used.

        return:: None
        """

        try:
            data_processing = Data_processing(start_month, end_month, year)
            logger.debug("data processing class loaded successfully")
            data_processing.run()

        except ValueError:
            logger.exception(
                "tere is definitely a PROBLEM in data processing -> check LOG!"
            )
        else:
            # removes files after transformation is complete is compleated.
            dir = "data/extracted_from_azure_raw"
            for file in os.listdir(dir):
                os.remove(os.path.join(dir, file))
            logger.info(
                "All files were successfuly transformed and extracted raw data removed."
            )

    def upload_transformed_data_to_azure(
        self, start_month: int, end_month: int, year: int
    ):
        """
        Uploads transformed data from local transformed_data folder to azure blob storage

        :param start_month:-> first month (int 1/2/3 etc.) to start transformations.
        :param end_month:-> last month (int (1/2/3/ etc)) to end transformations.
        :param year:-> year (int) of datasets to be used.

        return:: None

        """
        try:
            container_name = "transformed-yellow-taxi-data"
            try:
                self.blob_service_client.create_container(container_name)
                logger.debug(f"container {container_name} created successfully")
            except ConnectionRefusedError:
                logger.warning("container already exists, container client received")
            else:
                self.blob_service_client.get_container_client(container_name)
            raw_data_file_path = "data/transformed_data"
            months = [str(i).zfill(2) for i in range(start_month, end_month + 1)]

            for month in months:

                temp_filename = (
                    "clean_yellow_trip_data_" + str(year) + "-" + str(month) + ".csv"
                )
                logger.debug(f"{temp_filename} has been read")
                blob_client = self.blob_service_client.get_blob_client(
                    container=container_name, blob=temp_filename
                )
                logger.debug("blob lcient connection was done successfully")

                upload_file_path = os.path.join(raw_data_file_path, temp_filename)
                with open(upload_file_path, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
                logger.debug(f"inserting {temp_filename} to azure.", exc_info=1)
                logger.info(
                    f"{temp_filename} was succesfully inserted to {container_name}"
                )
        except ConnectionError or ValueError:
            logger.exception(
                f"there is problem uploading transformed data to azure blob storage"
            )
        else:
            # removes files after upload is compleated.
            dir = "data/transformed_data"
            for file in os.listdir(dir):
                os.remove(os.path.join(dir, file))

            logger.info(
                "ALL files were SUCCESSFULLY moved to transformed azure blob storage and removed from transformed_data folder."
            )

    def extract_transformed_taxi_data_from_azure(self):
        """
        Extracts all transformed data from azure blob storage to local folder

        :param: None

        return:: None
        """
        try:
            my_container = self.blob_service_client.get_container_client(
                "transformed-yellow-taxi-data"
            )
            logger.debug("container client retreived")
            list_of_blobs = my_container.list_blobs()
            logger.debug("list of blobs received")
            local_blob_path = "data/extracted_from_azure_transformed"
            logger.debug("local path created for blobs to be extracted")

            for blob in list_of_blobs:
                single_csv_file = (
                    my_container.get_blob_client(blob).download_blob().readall()
                )
                self.extract_single_taxi_blob(
                    blob.name, single_csv_file, local_blob_path
                )
                logger.info(f"blob exctarcted {blob.name}")
        except ValueError or ConnectionError:
            logger.exception(
                f"there something wrong extracting transformed taxi data from azure"
            )
        else:
            logger.info(
                "ALL transformed taxi data files were SUCCESSFULLY moved from azure"
            )

    def delete_blob(self, container_name: str, blob_name: str):
        """
        Deletes single blob from azure blob storage.

        :param: container_name:-> container name in blob storage as a string.
        :param: blob_name:-> blob name as a string.
        """
        try:
            my_container = self.blob_service_client.get_container_client(
                container=container_name
            )
            logger.debug(f"successfully connected to {container_name}")
            my_container.delete_blob(blob_name)
            logger.debug("there was an error deleting single blob")
        except ValueError or ConnectionError:
            logger.exception(
                "there was a problem deleting single blob. check container if correct {container_name}, {blob_name}"
            )
        else:
            logger.info("blob was deleted successfully!")
