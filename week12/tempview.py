from pyspark import SparkConf
from pyspark.sql import SparkSession
import logging

my_conf = SparkConf()
my_conf.set("spark.app.name", "assignment")
my_conf.set("spark.master","local[*]")


spark = SparkSession.builder.config(conf=my_conf).enableHiveSupport().getOrCreate()

orderDf = spark.read.format("csv") \
            .option("header", True)\
            .option("inferSchema", True)\
            .option("path","D:/TrendyTech/week12/orders.csv")\
            .load()

orderDf.createOrReplaceTempView("orders")

resultdf = spark.sql("select order_status , count(*) as total_orders from orders group by order_status")

resultdf.show()

spark.sql("create database if not exists retail")
orderDf.write.format("csv")\
    .mode("overwrite")\
    .bucketBy(4,"order_customer_id")\
    .saveAsTable("retail.orders3")