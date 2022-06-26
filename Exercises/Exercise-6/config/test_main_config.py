import unittest

from main_config import SparkOperation

from pyspark.sql.types import StructField, StringType, StructType
import pandas as pd


test_spark = SparkOperation('testing_app')
test_dir = 'testing'
data = [[1, 'a'], [2, 'b'], [3, 'c']]
pd_df = pd.DataFrame(data, columns=['Number', 'Letter'])
df = test_spark.spark.createDataFrame(pd_df)


class TestSum(unittest.TestCase):

    def test_create_schema(self):
        self.assertEqual(
            test_spark.create_schema(
                name_list=['col1', 'col2', 'col3'],
                type_list=[StringType(), StringType(), StringType()],
                nullable_list=[False, False, False]
            ),
            StructType([
                StructField('col1', StringType(), False,),
                StructField('col2', StringType(), False, ),
                StructField('col3', StringType(), False, )
            ])
        )

    def test_write_to_csv(self):
        self.assertEqual(
            test_spark.write_to_csv(df, test_dir),
            True
        )


if __name__ == '__main__':
    unittest.main()
