import pyspark
from pyspark.sql.session import SparkSession
spark = SparkSession.builder.appName("My First Dataframe").getOrCreate()

simple_dataframe = spark.createDataFrame(linhas, colunas)

