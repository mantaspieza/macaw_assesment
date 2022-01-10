import textwrap
import dotenv
import os
import pyodbc

dotenv.load_dotenv()

# specify the driver
driver = "{ODBC Driver 17 for SQL Server}"

# specify server name and  database name

server_name = "yellowtaxidata"
database_name = "yellow_taxi_database_2021"


server = f"yellowtaxidata.database.windows.net,1433"
username = os.getenv("USERNAME")
password = os.getenv("PASSWORD")

connection_string = textwrap.dedent(
    f"""
    Driver={driver};
    Server={server};
    Database={database_name};
    Uid={username};
    Pwd={password};
    Encrupt=yes;
    TrustServerCertificate=no;
    Connection Timeout=30;
"""
pyodbc.connect
# create a new PYODBC connectio object

conn = pyodbc.connect(connection_string)

# pyodbc.Cursor = cnxn.cursor()
# # define select query

# # select_sql = """
# #         CREATE TABLE test (first VARCHAR(20), second VARCHAR(20),
# #        third DATE);
# # """
# # crsr.execute(select_sql)

conn.close()
# print(driver)
