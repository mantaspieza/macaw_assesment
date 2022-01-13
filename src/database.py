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
        cursor = connection.cursor

        return connection, cursor

    def close_connection(self, connection: Any, cursor: Any) -> None:
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

    # def insert_from_dataframe(self):
    #     df = pd.read_csv(
    #         "data/extracted_from_azure_transformed/clean_yellow_trip_data_2021-01.csv"
    #     )

    #     conn = pyodbc.connect(self.connection_string)
    #     cursor = conn.cursor()
    #     MY_TABLE = "yellow_taxi_info_2021"
    #     # try:
    #     #     cursor.execute("""CREATE TABLE yellow_taxi_info_2021 (
    #     #         pickup_datetime DATETIME2,
    #     #         dropoff_datetime DATETIME2,
    #     #         passenger_count INT,
    #     #         );""")
    #     #     print('table created')
    #     # except:
    #     #     cursor.execute("""
    #     #     TRUNCATE TABLE yellow_taxi_info_2021
    #     #     """)
    #     #     print(' TRUNCATED')

    #     # conn.commit()
    #     # print('commited1')
    #     # cursor.fast_executemany = True
    #     # for index,row in df.iterrows():
    #     #     cursor.executemany("INSERT INTO yellow_taxi_info_2021 ([pickup_datetime],[dropoff_datetime],[passenger_count]) values (?,?,?) ", row['pickup_datetime'],row['dropoff_datetime'],row['passenger_count'])
    #     #     cursor.commit()

    #     # curosr.close()

    #     # conn.close()
    #     # print('connection closed')

    #     insert_to_tmp_tbl_stmt = f"INSERT INTO {MY_TABLE} VALUES (?,?,?)"

    #     cursor = conn.cursor()
    #     cursor.fast_executemany = True
    #     cursor.executemany(insert_to_tmp_tbl_stmt, df.values.tolist())
    #     print(f"{len(df)} rows inserted to the {MY_TABLE} table")
    #     cursor.commit()
    #     cursor.close()
    #     conn.close()
