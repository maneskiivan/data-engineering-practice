from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType, FloatType, TimestampType
import pandas as pd

from zipfile import ZipFile
from datetime import date
import os

from config.main_config import SparkOperation


def create_dir(path: str) -> str:
    # Create directory if there isn't one already
    if not os.path.isdir(path):
        os.mkdir(path)

    return path


def main():
    reports_dir = create_dir('reports')
    # read csv with pandas
    pd_df = None
    with ZipFile('data/Divvy_Trips_2019_Q4.zip') as z:
        with z.open('Divvy_Trips_2019_Q4.csv') as f:
            pd_df = pd.read_csv(f, parse_dates=['start_time', 'end_time'])

    # change data type
    pd_df['tripduration'] = pd_df['tripduration'].str.replace(',', '')
    pd_df['tripduration'] = pd.to_numeric(pd_df['tripduration'])

    # start a spark session
    ss = SparkOperation('Exercise6')

    column_names = pd_df.columns.values.tolist()

    types_list = [
        IntegerType(),
        TimestampType(),
        TimestampType(),
        IntegerType(),
        FloatType(),
        IntegerType(),
        StringType(),
        IntegerType(),
        StringType(),
        StringType(),
        StringType(),
        FloatType()
    ]
    nullable_list = [False, False, False, False, False, False, True, False, True, False, True, True]

    custom_schema = ss.create_schema(
        name_list=column_names,
        type_list=types_list,
        nullable_list=nullable_list
    )

    # create spark df from pandas df
    sp_df = ss.spark.createDataFrame(pd_df, schema=custom_schema)

    # ----- Answer questions -----

    # 1. What is the `average` trip duration per day?
    q1 = sp_df.groupBy(date_format(col('start_time'), 'yyyy-MM-dd').alias('Date')) \
        .agg(format_number(avg('tripduration'), 2).alias('Trip Duration per day')).orderBy('Date')

    # write result to csv
    ss.write_to_csv(q1, reports_dir + '/q1')

    # 2. How many trips were taken each day?
    q2 = sp_df.groupBy(date_format(col('start_time'), 'yyyy-MM-dd').alias('Date')) \
        .agg(count('trip_id').alias('Trips per day')).orderBy('Date')

    # write result to csv
    ss.write_to_csv(q2, reports_dir + '/q2')

    # 3. What was the most popular starting trip station for each month?
    q3 = sp_df.groupBy(date_format(col('start_time'), 'MM').alias('Month')) \
        .agg(max('from_station_name').alias('Most popular starting station name')).orderBy('Month')

    # write result to csv
    ss.write_to_csv(q3, reports_dir + '/q3')

    # 4. What were the top 3 trip stations each day for the last two weeks?
    window_spec = Window.partitionBy(date_format(col('start_time'), 'yyyy-MM-dd')).orderBy(col('from_station_name'))
    temp_df = sp_df.withColumn("rank", dense_rank().over(window_spec))

    q4 = temp_df.select(
        date_format(col('start_time'), 'yyyy-MM-dd').alias('Date'),
        col('from_station_name'),
        col('rank')
    ).distinct().filter(col("rank") <= 3).orderBy(desc('Date'), 'rank').limit(42)

    # write result to csv
    ss.write_to_csv(q4, reports_dir + '/q4')

    # 5. Do `Male`s or `Female`s take longer trips on average?
    q5 = sp_df.groupBy('gender').agg(format_number(avg('tripduration'), 2).alias('Avg trip duration')) \
        .orderBy(desc('Avg trip duration'))

    # write result to csv
    ss.write_to_csv(q5.na.replace('NaN', 'Not specified'), reports_dir + '/q5')

    # 6. What is the top 10 ages of those that take the longest trips, and shortest?
    today_date = date.today()
    current_year = float(today_date.year)

    temp_df1 = sp_df.na.drop(subset=['birthyear'])
    temp_df2 = temp_df1.withColumn('age', current_year - col('birthyear'))

    q6_longest = temp_df2.groupBy('age').agg(
        max('tripduration').alias('longest trip duration')
    ).orderBy(desc('longest trip duration'))

    q6_shortest = temp_df2.groupBy('age').agg(
        min('tripduration').alias('shortest trip duration')
    ).orderBy(asc('shortest trip duration'))

    # write result to csv
    ss.write_to_csv(q6_longest.limit(10), reports_dir + '/q6_longest')
    ss.write_to_csv(q6_shortest.limit(10), reports_dir + '/q6_shortest')


if __name__ == '__main__':
    main()
