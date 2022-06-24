from pyspark.sql import SparkSession, dataframe
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, TimestampType

import os


class SparkOperation:

    def __init__(self, appname: str):
        """
        Creates a spark session
        :param appname:Name for the spark session app
        """
        self.spark = SparkSession.builder.appName(appname) \
            .enableHiveSupport().getOrCreate()

    def create_schema(self, name_list: list, type_list: list, nullable_list: list):
        """
        Creates a custom schema to be used for a DataFrame.
        The lists must have equal number of items in them.
        :param name_list: List of names for the columns
        :param type_list: List of data types for the columns
        :param nullable_list: List of boolean values to confirm nullable or not
        :return: Struct type
        """
        fields = list()
        for i in range(len(name_list)):
            fields.append(StructField(name_list[i], type_list[i], nullable_list[i]))

        return StructType(fields)

    def write_to_csv(self, data, path):
        """
        Writes dataframe to csv and returns True if the file exists
        :param data: The dataframe
        :param path: The path of the file
        :return: bool
        """
        data.toPandas().to_csv(path, index=False)

        if os.path.exists(path):
            return True
