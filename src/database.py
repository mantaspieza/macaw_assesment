import textwrap
import os
import pyodbc
import pandas as pd


class database_interactions:
    def __init__(self) -> None:
        self.driver = "{ODBC Driver 17 for SQL Server}"
        self.servername = "yellowtaxidata"
        self.database_name = "yellow_taxi_database_2021"

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

    def create_connection_to_db(self):
        self.conn = pyodbc.connect(self.connection_string)

    def close_connection_to_db(self):
        self.conn.close()

    def insert_from_dataframe(self):
        df = pd.read_csv(
            "data/extracted_from_azure_transformed/clean_yellow_trip_data_2021-01.csv"
        )
        # df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"]).astype(str)
        # df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"]).astype(str)
        conn = pyodbc.connect(self.connection_string)
        cursor = conn.cursor()
        MY_TABLE = "yellow_taxi_info_2021"
        # try:
        #     cursor.execute("""CREATE TABLE yellow_taxi_info_2021 (
        #         pickup_datetime DATETIME2,
        #         dropoff_datetime DATETIME2,
        #         passenger_count INT,
        #         );""")
        #     print('table created')
        # except:
        #     cursor.execute("""
        #     TRUNCATE TABLE yellow_taxi_info_2021
        #     """)
        #     print(' TRUNCATED')

        # conn.commit()
        # print('commited1')
        # cursor.fast_executemany = True
        # for index,row in df.iterrows():
        #     cursor.executemany("INSERT INTO yellow_taxi_info_2021 ([pickup_datetime],[dropoff_datetime],[passenger_count]) values (?,?,?) ", row['pickup_datetime'],row['dropoff_datetime'],row['passenger_count'])
        #     cursor.commit()

        # curosr.close()

        # conn.close()
        # print('connection closed')

        insert_to_tmp_tbl_stmt = f"INSERT INTO {MY_TABLE} VALUES (?,?,?)"

        cursor = conn.cursor()
        cursor.fast_executemany = True
        cursor.executemany(insert_to_tmp_tbl_stmt, df.values.tolist())
        print(f"{len(df)} rows inserted to the {MY_TABLE} table")
        cursor.commit()
        cursor.close()
        conn.close()
