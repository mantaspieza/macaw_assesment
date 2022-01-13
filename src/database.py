import textwrap
import os
from typing import Any, Tuple
import pyodbc
import pandas as pd
import logging

from src.helper_functions import logger_setup

logger = logger_setup("database_interactions.log")


class database_interactions:
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
        try:
            if connection.is_connected():
                logger.debug("connection is stable")
                cursor.close()
                logger.debug("cursor was closed")
                connection.close()
                logger.debug("connection was terminated")
        except:
            logger.error("it was imposible to close connection check if it is open")
        else:
            logger.info("Connection was closed successfully")

    def create_table(self):
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
            logger.error(
                "there was an issue creating the table check if sql code is corect"
            )
        else:
            logger.info(f"table {self.table_name} was created correctly")

    def truncate_table(self):
        try:
            conn, cursor = self.create_connection()
            logger.debug("connection was succesful before truncating table")
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
        try:
            conn, cursor = self.create_connection()
            logger.debug("connection inserting data to sql was successful")
            cursor.fast_executemany = True

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

    # def get_average_passenger_count_between_two_dates(
    #     self, start_datetime_of_period: str, end_datetime_of_period: str
    # ):
    #     try:
    #         conn, cursor = self.create_connection()
    #         logger.debug('connected to database to get ')

    #         sql_code = f"""SELECT avg(cast(passenger_count as float))
    #                             from {self.table_name} as my_table
    #                                 where
    #                                     my_table.pickup_datetime >= {start_datetime_of_period}
    #                                     and
    #                                     my_table.dropoff_datetime <= {end_datetime_of_period}
    #                                     ;
    #                                 """
    #         result = cursor.execute(sql_code)

    #         print("The average between two selected dates is: ", result[0])

    #         self.close_connection(conn,cursor)

    def get_average_passenger_count_between_two_dates(
        self, start_datetime_of_period: str, end_datetime_of_period: str
    ):
        try:
            conn, cursor = self.create_connection()
            logger.debug("connection was successfull to sql database")

            sql_code = f"""SELECT avg(cast(passenger_count as float)) 
                            FROM {self.table_name} AS a 
                                WHERE a.pickup_datetime >= '{start_datetime_of_period}' 
                                    and
                                        a.dropoff_datetime <= '{end_datetime_of_period}' ;
                                    """
            cursor.execute(sql_code)
            logger.debug(
                "sql code executed successfully to retreive average customer number in time period"
            )
            records = cursor.fetchall()
            logger.debug("records were fetched successfully")

            self.close_connection(conn, cursor)
            logger.debug("connection was closed successfully")
        except ConnectionError or ValueError:
            logger.exception("connection or sql code failed. check code")
        finally:
            logger.info(
                f"average customer number in defined perdiod is {records[0][0]}"
            )
