import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


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
        temp_dataframe = temp_dataframe[temp_dataframe.passenger_count >= 0]

        return temp_dataframe

    def rename_columns(self, temp_dataframe: pd.DataFrame) -> pd.DataFrame:
        # print('paima')
        new_temp_dataframe = temp_dataframe.rename(
            columns={
                "tpep_pickup_datetime": "pickup_datetime",
                "tpep_dropoff_datetime": "dropoff_datetime",
            }
        )
        return new_temp_dataframe

    def read_csv_file(self, month: str) -> pd.DataFrame:
        temp_dataframe = pd.read_csv(
            f"extracted_from_azure/yellow_tripdata_{self.year}-{month}.csv",
            usecols=self.columns_to_extract,
            parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"],
        )
        # temp_dataframe_no_negative_count = self.remove_negative_passenger_count(temp_dataframe)
        # temp_df_fixed_namings = self.rename_columns(temp_dataframe_no_negative_count)

        return temp_dataframe

    def remove_outliers(self, temp_dataframe: pd.DataFrame, month: str) -> pd.DataFrame:

        if int(month) == 2:
            temp_dataframe = temp_dataframe[
                (temp_dataframe.pickup_datetime >= f"{self.year}-" + str(month) + "-01")
                & (
                    temp_dataframe.pickup_datetime
                    <= f"{self.year}-" + str(month) + "-28"
                )
            ]
        elif int(month) in [4, 6, 9, 11]:
            temp_dataframe = temp_dataframe[
                (temp_dataframe.pickup_datetime >= f"{self.year}-" + str(month) + "-01")
                & (
                    temp_dataframe.pickup_datetime
                    <= f"{self.year}-" + str(month) + "-30"
                )
            ]
        else:
            temp_dataframe = temp_dataframe[
                (temp_dataframe.pickup_datetime >= f"{self.year}-" + str(month) + "-01")
                & (
                    temp_dataframe.pickup_datetime
                    <= f"{self.year}-" + str(month) + "-31"
                )
            ]

        return temp_dataframe

    # def create_yellow_taxi_dataframe(self):

    def remove_extremely_short_and_long_rides(self, temp_dataframe: pd.DataFrame):
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

        return temp_dataframe

    def save_cleaned_csv_file(self, month, dataframe):
        dataframe.to_csv(
            f"transformed_data/clean_yellow_trip_data_{self.year}-"
            + str(month)
            + ".csv",
            index=False,
        )

    def run(self):
        for month in self.months:
            dataframe = self.read_csv_file(month=month)
            dataframe = self.remove_negative_passenger_count(dataframe)
            dataframe = self.rename_columns(dataframe)
            dataframe = self.remove_outliers(dataframe, month)
            dataframe = self.remove_extremely_short_and_long_rides(dataframe)
            self.save_cleaned_csv_file(month, dataframe)

    def test_if_correct(self):
        for i in self.months:
            self.yellow_taxi_dataframe = pd.concat(
                [
                    self.yellow_taxi_dataframe,
                    pd.read_csv(
                        f"transformed_data/clean_yellow_trip_data_{self.year}-{i}.csv"
                    ),
                ]
            )


# Data_processing(1,7,2021).run()

a = Data_processing(1, 2, 2021)
a.run()
a.test_if_correct()

a.yellow_taxi_dataframe.max()

# b = a.read_csv_file('01')
# c = a.remove_negative_passenger_count(b)
# d = a.rename_columns(c)
# e = a.remove_outliers(d,'01')
# f = a.remove_extremely_short_and_long_rides(e)
# a.save_cleaned_csv_file('01',f)