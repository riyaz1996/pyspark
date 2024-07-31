from pyspark import SparkConf
from pyspark.sql import SparkSession
import logging

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

my_conf = SparkConf()
my_conf.set("spark.app.name", "assignment")
my_conf.set("spark.master","local[*]")


spark = SparkSession.builder.config(conf=my_conf).enableHiveSupport().getOrCreate()

orderDf = spark.read.format("csv") \
            .option("inferSchema", True)\
            .option("path","D:/TrendyTech/week12/dataset1.csv")\
            .load()

df1 = orderDf.toDF("name","age","city")

def checkage(age):
    if age >= 18:
        return "y"
    else:
        return "n"

parseAgeFunction = udf(checkage,StringType())

df2 = df1.withColumn("adult",parseAgeFunction("age"))

df2.show()

