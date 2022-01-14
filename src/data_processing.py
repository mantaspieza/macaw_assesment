import pandas as pd
from pandas.core.frame import DataFrame
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    "%(asctime)s:%(levelname)s:%(name)s:%(funcName)s:%(message)s"
)

file_handler = logging.FileHandler("logs/data_processing.log", "w")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)


class Data_processing:
    """

    Class which performs transformations using raw data.

    ::Parameters::

    :: start_month_of_reports : first month for the csv file in raw data (currently 1)
    : end_month_of_report    : last month of the csv file of 2021 (currently 7)
    : year                   : year of the data used

    :: Functions ::

    :: remove_negative_passenger_count: -> removes rows where passenger count >= 0
    :: rename_columns   : -> renames columns to more convenient naming
    :: read_csv_file    : -> reads the csv file
    :: remove_outliers  : -> removes dates that are not in range of the month the dataset is built.
    :: remove_extremely_short_and_long_rides : -> removes rides where duration is less than 5 seconds and longer than 12hour shift.
    :: save_cleaned_csv_files: -> saves transformed dataframe
    :: run : -> general functionto combine it all



    """

    def __init__(
        self, start_month_of_report: int, end_month_of_report: int, year: int
    ) -> None:
        self.columns_to_extract = [
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
        ]
        self.year = year
        self.months = [
            str(i).zfill(2)
            for i in range(start_month_of_report, end_month_of_report + 1)
        ]
        self.yellow_taxi_dataframe = pd.DataFrame(
            columns=["pickup_datetime", "dropoff_datetime"]
        )

    def remove_negative_passenger_count(
        self, temp_dataframe: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Removes rows with negative passenger count.
        param: temp_dataframe -> pandas dataframe of the month the data needs to be adjusted.

        return:: temp_dataframe -> pandas dataframe with sorted values.
        """
        try:
            temp_dataframe = temp_dataframe[temp_dataframe.passenger_count >= 0]
        except ValueError:
            logger.exception(
                f"While removing negative passenger count in {temp_dataframe}"
            )
        else:
            logger.debug(f"successfully removed negative passenger count")

        return temp_dataframe

    def rename_columns(self, temp_dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Renames columns. removes "tpep" from the begining of the column name.
        param: temp_dataframe -> pandas dataframe holding correct values.

        return:: temp_dataframe -> pandas dataframe with renamed columns.
        """
        try:
            new_temp_dataframe = temp_dataframe.rename(
                columns={
                    "tpep_pickup_datetime": "pickup_datetime",
                    "tpep_dropoff_datetime": "dropoff_datetime",
                }
            )
        except ValueError:
            logger.exception(f"Problem changing collumn names at {temp_dataframe}")
        else:
            logger.debug(f"{temp_dataframe} columns were renamed correctly")
        return new_temp_dataframe

    def read_csv_file(self, month: str) -> pd.DataFrame:
        """
        Reads the raw csv files and prepares them for cleaning. parses dates, selects only columns required (picup/dropoff datetime and passenger count)

        param: month:str -> value of the month provided in two digits format eg. 01,02

        returns temp_dataframe -> pandas dataframe with selected columns.
        """
        try:
            temp_dataframe = pd.read_csv(
                f"data/extracted_from_azure_raw/yellow_tripdata_{self.year}-{month}.csv",
                usecols=self.columns_to_extract,
                parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"],
            )
            logger.debug("dataframe was successfully read to pandas")
        except ValueError:
            logger.exception(
                f"problem reading reding in dataframe month value -> {month}"
            )
        else:
            logger.debug(f"{month}.csv was read in correctly")

        return temp_dataframe

    def remove_outliers(self, temp_dataframe: pd.DataFrame, month: str) -> pd.DataFrame:
        """
        Removes monthly outliers. Checks whether the dates in pickup_datetime column are within the specific month.

        param: temo_dataframe -> pandas dataframe
        param: month -> str representation of the month in two digits. eg. 01, 02

        return:: pandas dataframe
        """
        try:
            if int(month) == 2:
                temp_dataframe = temp_dataframe[
                    (
                        temp_dataframe.pickup_datetime
                        >= f"{self.year}-" + str(month) + "-01"
                    )
                    & (
                        temp_dataframe.pickup_datetime
                        <= f"{self.year}-" + str(month) + "-28"
                    )
                ]
                logger.debug("february date outliers removed successfully")
            elif int(month) in [4, 6, 9, 11]:
                temp_dataframe = temp_dataframe[
                    (
                        temp_dataframe.pickup_datetime
                        >= f"{self.year}-" + str(month) + "-01"
                    )
                    & (
                        temp_dataframe.pickup_datetime
                        <= f"{self.year}-" + str(month) + "-30"
                    )
                ]
                logger.debug("months with 30 days has their outliers removed")
            else:
                temp_dataframe = temp_dataframe[
                    (
                        temp_dataframe.pickup_datetime
                        >= f"{self.year}-" + str(month) + "-01"
                    )
                    & (
                        temp_dataframe.pickup_datetime
                        <= f"{self.year}-" + str(month) + "-31"
                    )
                ]
                logger.debug("mohts with 31 day has their outliers removed!")
        except ValueError:
            logger.exception(
                f"There was a problem removing outliers in {temp_dataframe}"
            )
        else:
            logger.debug(f"ALL outliers removed successfully")

        return temp_dataframe

    def remove_extremely_short_and_long_rides(self, temp_dataframe: pd.DataFrame):
        """
        Removes trips which are shorter than 5 seconds and longer than 12hours (as its the typical shift for a driver)

        param: temp_dataframe -> pandas dataframe

        return: pandas dataframe
        """
        try:
            trip_duration_in_seconds = (
                (temp_dataframe.dropoff_datetime - temp_dataframe.pickup_datetime)
                .astype("timedelta64[s]")
                .astype(int)
            )
            logger.debug("trip duration was calculated successfully")
            temp_dataframe["trip_duration_in_seconds"] = trip_duration_in_seconds
            logger.debug("Trip duration column was created successfully")
            temp_dataframe = temp_dataframe[
                (
                    temp_dataframe.trip_duration_in_seconds > 5
                )  # as 5 second trip migh be just a mistake
                & (
                    temp_dataframe.trip_duration_in_seconds < 43200
                )  # 12 hours shifts is a common standard in ny taxies
            ].drop("trip_duration_in_seconds", axis=1)
            logger.debug("extremely short rides and extremely long rides trimmed")
        except ValueError:
            logger.exception(
                f"There was an error restricting trip duration in {temp_dataframe}"
            )
        else:
            logger.debug("Trip duration outliers successfuly removed")
        return temp_dataframe

    def save_cleaned_csv_file(self, month, dataframe: pd.DataFrame) -> None:
        """
        Saves cleaned and transformed dataframe to transformed dataframe folder

        param: month -> two digit str representation of a month, eg. 01, 02
        param: dataframe -> pandas dataframe

        return:: None
        """
        try:
            dataframe.to_csv(
                f"data/transformed_data/clean_yellow_trip_data_{self.year}-"
                + str(month)
                + ".csv",
                index=False,
            )
        except ValueError:
            logger.exception(f"There was a problem saving to csv {dataframe}")
        else:
            logger.debug(f"{dataframe} was saved successfully")

    def run(self):
        """
        Function which iterates from start to end month applying all functions of the class.

        param: None

        return:: None
        """
        try:
            for month in self.months:
                dataframe = self.read_csv_file(month=month)
                logger.info(f"{self.year}-{month} dataframe was successfully read in")
                dataframe = self.remove_negative_passenger_count(dataframe)
                dataframe = self.rename_columns(dataframe)
                dataframe = self.remove_outliers(dataframe, month)
                dataframe = self.remove_extremely_short_and_long_rides(dataframe)
                self.save_cleaned_csv_file(month, dataframe)
                logger.info(
                    f"{self.year}-{month} dataframe was successfully cleaned and saved"
                )
        except ValueError:
            logger.exception(
                "there was an error running run() command, check logs for debug level"
            )
        finally:
            logger.info(
                "ALL dataframes were successfuly transformed and saved. Congratz"
            )
