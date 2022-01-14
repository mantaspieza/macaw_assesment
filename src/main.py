import logging
from os import error

from src.ETL import ETL
from src.data_processing import Data_processing
from src.database import Database_interactions

etl = ETL()
# data_processing = Data_processing(1,7,2021)
database_interactions = Database_interactions()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    "%(asctime)s:%(levelname)s:%(name)s:%(funcName)s:%(message)s"
)

file_handler = logging.FileHandler("logs/main.log", "w")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)


class Perform_the_assesment_task:
    """
    Class which performs all required spets to implement ETL, upload data to azure sql database and return result of the average passenger count in specific period. (it can be adjusted in the code)

    ::Functions::
    :: go:: runs all the steps in order.

    ::Parameters::
    :: None::

    return:: None
    """

    def __init__(self) -> None:
        """
        Period ( for average pasenger count) start date and end date can be adjusted here for general querry.
        """
        self.start_month = 1
        self.end_month = 7
        self.year = 2021
        self.period_start_date = "2021-02-14 00:00:00"
        self.period_end_date = "2021-07-12 23:21:12"

    def go(self):
        try:
            etl.upload_raw_data_to_azure(
                start_month=self.start_month, end_month=self.end_month
            )
            logger.info(">>> RAW DATA UPLOADED TO AZURE BLOB STORAGE <<<")
            etl.extract_raw_taxi_data_from_azure()
            logger.info(">>> RAW DATA EXTRACTED FROM AZURE BLOB STORAGE <<<")
            etl.transform_raw_data(
                start_month=self.start_month, end_month=self.end_month, year=2021
            )
            logger.info(">>> RAW DATA SUCCESSFULLY TRANSFORMED <<<")
            etl.upload_transformed_data_to_azure(
                start_month=self.start_month, end_month=self.end_month, year=2021
            )
            logger.info(
                ">>> TRANSFORMED DATA SUCCESSFULLY UPLOADED TO AZURE BLOB STORAGE"
            )
            etl.extract_transformed_taxi_data_from_azure()
            logger.info(">>> TRANSFORMED DATA SUCCESSFULLY EXTRACTED FROM DATABASE <<<")

            try:
                database_interactions.create_table()
                logger.info(">> SQL TABLE CREATED <<<")
            except:
                logger.info(">>> SQL TABLE WILL BE CLEARED <<<")
            else:
                database_interactions.truncate_table()
                logger.info(">>> SQL TABLE WAS CLEARED")
            database_interactions.insert_transformed_data_to_sql_db()
            logger.info(">>> ALL TRANSFORMED DATA MOVED TO SQL DATABASE <<<")

            database_interactions.get_average_passenger_count_between_two_dates(
                start_datetime_of_period=self.period_start_date,
                end_datetime_of_period=self.period_end_date,
            )
            logger.info(">>> THE RESULT IS ABOVE THIS LINE <<<")
        except error:
            logger.exception("There is something wrong. check the logs!")
        else:
            logger.info(">>>>> yay. all went well! <<<<<<<<")
