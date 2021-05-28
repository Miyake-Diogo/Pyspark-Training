from pyspark import SparkConf
from pyspark.sql import SparkSession

#data.write \
#    .format("jdbc") \
#    .option("url", "jdbc:postgresql:dbserver") \
#    .option("dbtable", "schema.tablename") \
#    .option("user", "username") \
#    .option("password", "password") \
#    .save()


if __name__ == '__main__':
    spark_conf = SparkConf().setAppName(f'Spark-Streaming')
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

    conta = spark.read\
        .format("kafka")\
        .option("kafka.bootstrap.servers", '10.92.5.8:9092,10.92.5.5:9092,10.92.5.6:9092,10.92.5.9:9092') \
        .option("subscribe", 'conta')\
        .option("startingOffsets", "earliest")\
        .orderBy('timestamp', ascending=False)\
        .load().selectExpr("CAST(value AS STRING)", "timestamp")

    print(conta.printSchema())

    print(conta.show(truncate=False))
