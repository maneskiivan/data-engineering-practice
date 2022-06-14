from pyspark.sql import SparkSession
from pyspark.sql.functions import *
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
  q1 = sp_df.groupBy(date_format(col('start_time').alias('Date'), 'yyyy-MM-dd')) \
      .avg('tripduration').alias('Average trip duration')

  print(q1.show())
  test

if __name__ == '__main__':
    main()
