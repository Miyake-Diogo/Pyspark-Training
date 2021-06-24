import unittest
import pyspark
from pyspark.sql.session import SparkSession
spark = SparkSession.builder.appName("My_tests").getOrCreate()

class simple_transformation_data(unittest.TestCase):

    def read_blank_csv_data_return_none(self):
        csv_dataframe = spark.read.format("csv").load("path")
        self.assertEqual(csv_dataframe, None)

    def read_csv_return_dataframe