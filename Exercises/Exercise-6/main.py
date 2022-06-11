from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, TimestampType
import pandas as pd

from zipfile import ZipFile


def main():
  spark = SparkSession.builder.appName('Exercise6') \
    .enableHiveSupport().getOrCreate()

  # read csv with pandas
  pd_df = None
  with ZipFile('data/Divvy_Trips_2019_Q4.zip') as z:
    with z.open('Divvy_Trips_2019_Q4.csv') as f:
      pd_df = pd.read_csv(f)

  # create spark df from pandas df
  # Need to add schema
  custom_schema = StructType(
    [
      StructField('trip_id', IntegerType, False),
      StructField('start_time', TimestampType, False),
      StructField('end_time', TimestampType, False),
      StructField('bikeid', IntegerType, False),
      StructField('tripduration', IntegerType, False),
      StructField('from_station_id', IntegerType, False),
      StructField('from_station_name', StringType, True)
    ]
  )
  sp_df = spark.createDataFrame(pd_df.astype(str))




if __name__ == '__main__':
    main()
