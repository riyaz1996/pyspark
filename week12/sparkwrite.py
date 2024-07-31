from pyspark import SparkConf
from pyspark.sql import SparkSession
import logging

my_conf = SparkConf()
my_conf.set("spark.app.name", "assignment")
my_conf.set("spark.master","local[*]")
# my_conf.set("spark.jars","D:/TrendyTech/week12/spark-avro_2.12-2.4.4.jar")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

orderDf = spark.read.format("csv") \
            .option("header", True)\
            .option("inferSchema", True)\
            .option("path","D:/TrendyTech/week12/orders.csv")\
            .load()


print("number of partitions are ", orderDf.rdd.getNumPartitions())
ordersRep = orderDf.repartition(4)


ordersRep.write.format("csv")\
.mode("overwrite")\
.option("path","D:/TrendyTech/week12/submission/")\
.save()