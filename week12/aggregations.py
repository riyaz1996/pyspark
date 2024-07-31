from pyspark import SparkConf
from pyspark.sql import SparkSession
import logging
from pyspark.sql.functions import *

my_conf = SparkConf()
my_conf.set("spark.app.name", "assignment")
my_conf.set("spark.master","local[*]")


spark = SparkSession.builder.config(conf=my_conf).enableHiveSupport().getOrCreate()

invoiceDf = spark.read.format("csv") \
            .option("header", True)\
            .option("inferSchema", True)\
            .option("path","D:/TrendyTech/week12/order_data.csv")\
            .load()



# invoiceDf.select(count("*").alias("RowCount")
#                  ,sum("Quantity").alias("TotalQuantity")
#                  ,avg("UnitPrice").alias("AvgPrice")
#                  ,countDistinct("InvoiceNo").alias("CountDistinct")).show()
#
#
# invoiceDf.selectExpr("count(*) as RowCount"
#                      ,"sum(Quantity) as TotalQuantity"
#                      ,"avg(UnitPrice) as AvgPrice"
#                      ,"count(Distinct(InvoiceNo)) as CountDistinct").show()
#
# invoiceDf.createOrReplaceTempView("sales")
# #
# spark.sql("select count(*),sum(Quantity),avg(UnitPrice),count(distinct(InvoiceNo)) from sales").show()
#


invoiceDf.groupBy("Country","InvoiceNo")\
.agg(sum("Quantity").alias("TotalQuantity")
    ,avg("UnitPrice").alias("AvgPrice")
    ,countDistinct("InvoiceNo").alias("CountDistinct")).show()


#
invoiceDf.groupBy("Country", "InvoiceNo")\
    .agg(expr("sum(Quantity) as TotalQunatity"),
    expr("sum(Quantity * UnitPrice) as InvoiceValue")).show()

invoiceDf.createOrReplaceTempView("sales")

spark.sql("select  country,InvoiceNo,sum(Quantity) as totQty,sum(Quantity * UnitPrice) as InvoiceValue from sales group by country,InvoiceNo").show()