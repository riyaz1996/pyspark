from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
import logging
from pyspark.sql.functions import *

my_conf = SparkConf()
my_conf.set("spark.app.name", "assignment")
my_conf.set("spark.master","local[*]")


spark = SparkSession.builder.config(conf=my_conf).enableHiveSupport().getOrCreate()

df1 = spark.read.format("csv") \
            .option("header", True)\
            .option("inferSchema", True)\
            .option("path","D:/TrendyTech/week12/windowdata.csv")\
            .load()

myWindow = Window.partitionBy("country")\
            .orderBy("weeknum")\
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)

mydf = df1.withColumn("RunningTotal",sum("invoicevalue").over(myWindow))
mydf = mydf.drop("invoicevalue")
mydf.show()