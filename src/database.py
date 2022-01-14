import textwrap
import os
from typing import Any, Tuple
import pyodbc
import pandas as pd
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    "%(asctime)s:%(levelname)s:%(name)s:%(funcName)s:%(message)s"
)

file_handler = logging.FileHandler("logs/database_interactions.log", "w")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)


class Database_interactions:
    """
    Class which interacts with azure sql database

    ::Requirements (hidden in env file)::
    :: ODBC Driver 17
    :: servername
    :: database_name
    :: table_name

    ::Functions::
    :: create_connection:-> creates connection to database
    :: close_connection:-> closes connection to database
    :: create_table:-> creates specific table for this task in sql database.
    :: truncate_table:-> clears all values in the table if the table is created.
    :: insert_transformed_data_to_sql_db: -> inserts transformed data to sql database in azure
    :: get_average_passenger_count_between_two_dates:-> returns the average passenger count in specified time period.


    return:: None
    """

    def __init__(self) -> None:
        self.driver = "{ODBC Driver 17 for SQL Server}"
        self.servername = "yellowtaxidata"
        self.database_name = "yellow_taxi_database_2021"
        self.table_name = "yellow_taxi_info_2021"

        self.server = "yellowtaxidata.database.windows.net,1433"
        self.username = os.getenv("USERNAME")
        self.password = os.getenv("PASSWORD")

        self.connection_string = textwrap.dedent(
            f"""
    Driver={self.driver};
    Server={self.server};
    Database={self.database_name};
    Uid={self.username};
    Pwd={self.password};
    Encrypt=yes;
    TrustServerCertificate=no;
    Connection Timeout=30;
"""
        )

    def create_connection(self) -> Tuple:
        """
        Creates connection to azure sql database.
        :param: None

        return:: connection, cursor
        """

        try:
            connection = pyodbc.connect(self.connection_string)
            logger.debug("database connection established")
            cursor = connection.cursor()
            logger.debug("cursor set succeeded")

        except ConnectionError:
            logger.exception("Connection was rejected, please check your credentials")
        else:
            logger.info("successfuly connected to database")

        return connection, cursor

    def close_connection(self, connection: Any, cursor: Any) -> None:
        """
        Closes connection to sql database.

        :param: connection:-> variable holdng connection to database (from function create_connection)
        :param: cursor:-> variable holding cursor to sql database (from function create_connection)

        return:: None
        """

        try:
            if connection.is_connected():
                logger.debug("connection is stable")
                cursor.close()
                logger.debug("cursor was closed")
                connection.close()
                logger.debug("connection was terminated")
        except:
            logger.exception("it was imposible to close connection check if it is open")
        else:
            logger.info("Connection was closed successfully")

    def create_table(self):
        """
        Creates table "yellow_taxi_info_2021" in azure sql database.

        :Param:: None

        return:: None
        """

        try:
            conn, cursor = self.create_connection()
            logger.debug("connectio creating table was successfull")

            sql_code = f"""
                    CREATE TABLE {self.table_name} (
                    pickup_datetime DATETIME2,
                    dropoff_datetime DATETIME2,
                    passenger_count INT,
                    ); """

            cursor.execute(sql_code)
            logger.debug("table was successfully created")
            cursor.commit()
            logger.debug("table was successfully commited to database")
            self.close_connection(conn, cursor)
            logger.debug("connection was terminated succesfuly")
        except:
            logger.exception(
                "there was an issue creating the table check if sql code is corect"
            )
        else:
            logger.info(f"table {self.table_name} was created correctly")

    def truncate_table(self):
        """
        clears all existing values in azure sql database.

        :param: None

        return:: None
        """
        conn, cursor = self.create_connection()
        logger.debug("connection was succesful before truncating table")
        try:
            sql_code = f"TRUNCATE TABLE {self.table_name}"

            cursor.execute(sql_code)
            logger.debug("cursor execute truncate sql code")

            cursor.commit()
            logger.debug("table was successfuly truncated")

            self.close_connection(connection=conn, cursor=cursor)
            logger.debug("connection was terminated successfuly")
        except:
            logger.exception(
                f"there was an connection or value error truncating the {self.table_name}"
            )
        else:
            logger.info(f"{self.table_name} was successfully truncated")

    def insert_transformed_data_to_sql_db(self) -> None:
        """
        Inserts transformed data from local extracted_from_azure_transformed folder to azure sql datbase.

        :param: None

        return: None
        """
        conn, cursor = self.create_connection()
        logger.debug("connection inserting data to sql was successful")
        cursor.fast_executemany = True
        try:
            dirListing = os.listdir("data/extracted_from_azure_transformed/")
            logger.debug("directory path described correctly")

            sql_code = f"INSERT INTO {self.table_name} VALUES (?,?,?)"

            for i in range(1, len(dirListing) + 1):
                df = pd.read_csv(
                    "data/extracted_from_azure_transformed/clean_yellow_trip_data_2021-0"
                    + str(i)
                    + ".csv"
                )
                logger.debug(f"dataframe 2021-0" + str(i) + "was read succesfully")
                cursor.executemany(sql_code, df.values.tolist())
                logger.debug(
                    "cursor succesfully executed sql code and read data from dataframe"
                )
                cursor.commit()
                logger.debug("sql code was successfuly commited")
                logger.info(
                    "dataframe 2021-0"
                    + str(i)
                    + "successfuly commited to azure sql database"
                )

            self.close_connection(conn, cursor)
        except ConnectionError or ValueError:
            logger.exception(
                "there is connection or value problem inserting transformed data to sql"
            )
        else:
            logger.info("all dataframes were successfully moved to azure sql database")

    def get_average_passenger_count_between_two_dates(
        self, start_datetime_of_period: str, end_datetime_of_period: str
    ):
        """
        Connects to azure sql database and retreives average passenger count between two dates.

        :param: start_datetime_of_period:-> string datetime value of beginning of period. eg. 2021-01-02 00:21:34
        :param: end_datetime_of_period:-> string datime value of the end of period. eg. 2021-06-24 04:12:22

        return:: average passenger count between two dates.
        """
        conn, cursor = self.create_connection()
        logger.debug("connection was successfull to sql database")
        try:
            sql_code = f"""SELECT avg(cast(passenger_count as float)) 
                            FROM {self.table_name} AS a 
                                WHERE a.pickup_datetime >= '{start_datetime_of_period}' 
                                    and
                                        a.dropoff_datetime <= '{end_datetime_of_period}' ;
                                    """
            cursor.execute(sql_code)
            logger.info(
                "sql code executed successfully to retreive average customer number in time period"
            )
            records = cursor.fetchall()
            logger.debug("records were fetched successfully")

            self.close_connection(conn, cursor)
            logger.info("connection was closed successfully")
        except ConnectionError or ValueError:
            logger.exception("connection or sql code failed. check code")
        finally:
            logger.info(
                f"average customer number in defined perdiod is {records[0][0]}"
            )
