from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, TimestampType
import pandas as pd

from zipfile import ZipFile


def main():
  # read csv with pandas
  pd_df = None
  with ZipFile('data/Divvy_Trips_2019_Q4.zip') as z:
    with z.open('Divvy_Trips_2019_Q4.csv') as f:
      pd_df = pd.read_csv(f, parse_dates=['start_time', 'end_time'])

  # change data type for tripduration column from string to float
  pd_df['tripduration'] = pd_df['tripduration'].str.replace(',', '')
  pd_df['tripduration'] = pd.to_numeric(pd_df['tripduration'])

  # start a spark session
  spark = SparkSession.builder.appName('Exercise6') \
      .enableHiveSupport().getOrCreate()

  # create spark df from pandas df
  custom_schema = StructType(
    [
      StructField('trip_id', IntegerType(), False),
      StructField('start_time', TimestampType(), False),
      StructField('end_time', TimestampType(), False),
      StructField('bikeid', IntegerType(), False),
      StructField('tripduration', FloatType(), False),
      StructField('from_station_id', IntegerType(), False),
      StructField('from_station_name', StringType(), True),
      StructField('to_station_id', IntegerType(), False),
      StructField('to_station_name', StringType(), True),
      StructField('usertype', StringType(), False),
      StructField('gender', StringType(), True),
      StructField('birthyear', FloatType(), False)
    ]
  )

  sp_df = spark.createDataFrame(pd_df, schema=custom_schema)

  # ----- Answer questions -----

  # 1. What is the `average` trip duration per day?
  q1 = sp_df.groupBy(date_format(col('start_time'), 'yyyy-MM-dd').alias('Date')) \
      .agg(format_number(avg('tripduration'), 2).alias('Trip Duration per day')).orderBy('Date')

  q1.show()

  # 2. How many trips were taken each day?
  q2 = sp_df.groupBy(date_format(col('start_time'), 'yyyy-MM-dd').alias('Date')) \
      .agg(count('trip_id').alias('Trips per day')).orderBy('Date')

  q2.show()

  # 3. What was the most popular starting trip station for each month?
  q3 = sp_df.groupBy(date_format(col('start_time'), 'MM').alias('Month')) \
      .agg(max('from_station_name').alias('Most popular starting station name')).orderBy('Month')

  q3.show()

  # 4. What were the top 3 trip stations each day for the last two weeks?
  window_spec = Window.partitionBy(date_format(col('start_time'), 'yyyy-MM-dd')).orderBy(col('from_station_name'))
  q4 = sp_df.withColumn("rank", dense_rank().over(window_spec))

  q4.select(
    date_format(col('start_time'), 'yyyy-MM-dd').alias('Date'),
    col('from_station_name'),
    col('rank')
  ).distinct().filter(col("rank") <= 3).orderBy(desc('Date'), 'rank').limit(42).show()

  # 5. Do `Male`s or `Female`s take longer trips on average?
  q5 = sp_df.groupBy('gender').agg(format_number(avg('tripduration'), 2).alias('Avg trip duration')) \
      .orderBy(desc('Avg trip duration'))

  q5.na.replace('NaN', 'Not specified').show()

  # 6. What is the top 10 ages of those that take the longest trips, and shortest?


if __name__ == '__main__':
    main()
