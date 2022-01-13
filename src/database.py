import textwrap
import os
from typing import Any, Tuple
import pyodbc
import pandas as pd


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
        connection = pyodbc.connect(self.connection_string)
        cursor = connection.cursor()

        return connection, cursor

    def close_connection(self, connection: Any, cursor: Any) -> None:
        if connection.is_connected():
            cursor.close()
            connection.close()

    def create_table(self):
        conn, cursor = self.create_connection()

        sql_code = f"""
                CREATE TABLE {self.table_name} (
                pickup_datetime DATETIME2,
                dropoff_datetime DATETIME2,
                passenger_count INT,
                ); """

        cursor.execute(sql_code)
        cursor.commit()

        self.close_connection(conn, cursor)

    def truncate_table(self):
        conn, cursor = self.create_connection()

        sql_code = f"TRUNCATE TABLE {self.table_name}"

        cursor.execute(sql_code)
        cursor.commit()

        self.close_connection(connection=conn, cursor=cursor)

    def insert_transformed_data_to_sql_db(self) -> None:
        conn, cursor = self.create_connection()
        cursor.fast_executemany = True

        dirListing = os.listdir("data/extracted_from_azure_transformed/")

        sql_code = f"INSERT INTO {self.table_name} VALUES (?,?,?)"

        for i in range(1, len(dirListing) + 1):
            df = pd.read_csv(
                "data/extracted_from_azure_transformed/clean_yellow_trip_data_2021-0"
                + str(i)
                + ".csv"
            )
            cursor.executemany(sql_code, df.values.tolist())
            cursor.commit()

        self.close_connection(conn, cursor)

    def get_average_passenger_count_between_two_dates(
        self, start_datetime_of_period: str, end_datetime_of_period: str
    ):
        conn, cursor = self.create_connection()

        sql_code = f"""SELECT avg(cast(passenger_count as float))
                             from {self.table_name} as my_table
                                 where 
                                    my_table.pickup_datetime >= {start_datetime_of_period}
                                    and
                                    my_table.dropoff_datetime <= {end_datetime_of_period}
                                    ;
                                 """
        result = cursor.execute(sql_code)

        print("The average between two selected dates is: ", result[0])

    def get_average_passenger_count_between_two_dates(
        self, start_datetime_of_period: str, end_datetime_of_period: str
    ):
        conn, cursor = self.create_connection()
        try:
            print("connected")
            print(type(start_datetime_of_period))
            print(end_datetime_of_period)

            sql_code = f"""select avg(cast(passenger_count as float)) from {self.table_name} as a where a.pickup_datetime > '{start_datetime_of_period}' and a.dropoff_datetime < '{end_datetime_of_period}' ;
                                    """
            cursor.execute(sql_code)
            print("code_executed")
            records = cursor.fetchall()

            print(
                "average number of passengers between two dates: ",
                round(records[0][0], 4),
            )
        except:
            self.close_connection(conn, cursor)
        finally:
            self.close_connection(conn, cursor)
