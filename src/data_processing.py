import pandas as pd
from pandas.core.frame import DataFrame
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    "%(asctime)s:%(levelname)s:%(name)s:%(funcName)s:%(message)s"
)

file_handler = logging.FileHandler("logs/data_processing.log", "w")
file_handler.setLevel(logging.logging.ERROR)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)


class Data_processing:
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
        try:
            temp_dataframe = temp_dataframe[temp_dataframe.passenger_count >= 0]
        except ValueError:
            logger.exception(
                f"While removing negative passenger count in {temp_dataframe}"
            )
        finally:
            logger.debug("Passenger count was limited >0 successfully")

        return temp_dataframe

    def rename_columns(self, temp_dataframe: pd.DataFrame) -> pd.DataFrame:

        try:
            new_temp_dataframe = temp_dataframe.rename(
                columns={
                    "tpep_pickup_datetime": "pickup_datetime",
                    "tpep_dropoff_datetime": "dropoff_datetime",
                }
            )
        except ValueError:
            logger.exception(f"Problem changing collumn names at {temp_dataframe}")
        finally:
            logger.debug(f"{temp_dataframe} columns were renamed correctly")
        return new_temp_dataframe

    def read_csv_file(self, month: str) -> pd.DataFrame:
        try:
            temp_dataframe = pd.read_csv(
                f"data/extracted_from_azure_raw/yellow_tripdata_{self.year}-{month}.csv",
                usecols=self.columns_to_extract,
                parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"],
            )
        except ValueError:
            logger.exception(
                f"problem reading reding in dataframe month value > {month}"
            )
        finally:
            logger.debug(f"{month}.csv was read in correctly")

        return temp_dataframe

    def remove_outliers(self, temp_dataframe: pd.DataFrame, month: str) -> pd.DataFrame:

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
        except ValueError:
            logger.exception(
                f"There was a problem removing outliers in {temp_dataframe}"
            )
        finally:
            logger.debug(f"Outliers removed in {temp_dataframe}")

        return temp_dataframe

    # def create_yellow_taxi_dataframe(self):

    def remove_extremely_short_and_long_rides(self, temp_dataframe: pd.DataFrame):

        try:
            trip_duration_in_seconds = (
                (temp_dataframe.dropoff_datetime - temp_dataframe.pickup_datetime)
                .astype("timedelta64[s]")
                .astype(int)
            )
            temp_dataframe["trip_duration_in_seconds"] = trip_duration_in_seconds

            temp_dataframe = temp_dataframe[
                (
                    temp_dataframe.trip_duration_in_seconds > 5
                )  # as 5 second trip migh be just a mistake
                & (
                    temp_dataframe.trip_duration_in_seconds < 43200
                )  # 12 hours shifts is a common standard in ny taxies
            ].drop("trip_duration_in_seconds", axis=1)
        except ValueError:
            logger.exception(
                f"There was an error restricting trip duration in {temp_dataframe}"
            )
        finally:
            logger.debug("Trip duration outliers successfuly removed")
        return temp_dataframe

    # def correct_datetime_strings_for_database_input(self, temp_dataframe: pd.DataFrame):

    #     try:
    #         temp_dataframe["pickup_datetime"] = (
    #             temp_dataframe["pickup_datetime"].astype(str).str.replace(" ", "T")
    #         )
    #         temp_dataframe["dropoff_datetime"] = (
    #             temp_dataframe["dropoff_datetime"].astype(str).str.replace(" ", "T")
    #         )
    #         temp_dataframe["passenger_count"] = temp_dataframe.passenger_count.astype(int)
    #     return temp_dataframe

    def save_cleaned_csv_file(self, month, dataframe: pd.DataFrame):

        try:
            dataframe.to_csv(
                f"data/transformed_data/clean_yellow_trip_data_{self.year}-"
                + str(month)
                + ".csv",
                index=False,
            )
        except ValueError:
            logger.exception(f"There was a problem saving to csv {dataframe}")
        finally:
            logger.debug(f"{dataframe} was saved successfully")

    def run(self):
        try:
            for month in self.months:
                dataframe = self.read_csv_file(month=month)
                logger.info(f"{self.year}-{month} dataframe was successfully read in")
                dataframe = self.remove_negative_passenger_count(dataframe)
                dataframe = self.rename_columns(dataframe)
                dataframe = self.remove_outliers(dataframe, month)
                dataframe = self.remove_extremely_short_and_long_rides(dataframe)
                # dataframe = self.correct_datetime_strings_for_database_input(dataframe)
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

    # def test_if_correct(self):
    #     for i in self.months:
    #         self.yellow_taxi_dataframe = pd.concat(
    #             [
    #                 self.yellow_taxi_dataframe,
    #                 pd.read_csv(
    #                     f"./data/transformed_data/clean_yellow_trip_data_{self.year}-{i}.csv"
    #                 ),
    #             ]
    #
