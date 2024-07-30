from pyspark import SparkConf
from pyspark.sql import SparkSession



my_conf = SparkConf()
my_conf.set("spark.app.name", "my first application")
my_conf.set("spark.master","local[*]")


orderddl = "orderid integer, orderdate timestamp, customerid integer ,status string "

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

orderDf = spark.read.format("csv")\
    .option("header",True)\
    .schema(orderddl)\
    .option("path","orders.csv")\
    .load()


groupedDf = orderDf.repartition(4)\
.where("customerid > 10000")\
.select("orderid","customerid")\
.groupBy("customerid")\
.count()

groupedDf.show()

spark.stop()