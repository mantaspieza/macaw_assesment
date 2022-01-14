## Macaw Data Engineering Academy Assesment

#### General Info

Yellow taxi data is downloaded from the site and uploaded to data/raw_data folder.

Only available data for 2021 is used. ~13.5M rows.

#### Pipeline

- raw taken from raw_data file
- raw data uploaded to azure blob server and deleted from local folder
- raw data downloaded from azure blob server
- raw data cleaned and transformed and saved localy
- transformed data uploaded to azure blob server and removed from local folder
- transformed data downloaded from azure blob server
- table in sql database created / cleared if exists
- transformed data is uploaded to sql database table. removed from local file
- querry formulated to retreive average passenger count between two dates

#### Logs are available to see how the pipeline works.

#### Things left out:

- quality functions is not written due to lack of time.
