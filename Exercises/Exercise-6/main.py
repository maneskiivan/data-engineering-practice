from pyspark.sql import SparkSession
import pandas as pd

from zipfile import ZipFile
import io


def main():
  spark = SparkSession.builder.appName('Exercise6') \
    .enableHiveSupport().getOrCreate()

  pd_df = None
  with ZipFile('data/Divvy_Trips_2019_Q4.zip') as z:
    with z.open('Divvy_Trips_2019_Q4.csv') as f:
      pd_df = pd.read_csv(f)

  sp_df = spark.createDataFrame(pd_df.astype(str))
  print(sp_df.show())



if __name__ == '__main__':
    main()
