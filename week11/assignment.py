from logging import Logger

from pyspark import SparkConf
from pyspark.sql import SparkSession
import logging

my_conf = SparkConf()
my_conf.set("spark.app.name", "assignment")
my_conf.set("spark.master","local[*]")



spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

logger = logging.getLogger("org")

logger.setLevel(logging.ERROR)

windowdataSchema = "country String, weeknum integer, numinvoices integer,totalquantity  integer,invoicevalue float"


window_data = spark.read.format("csv")\
              .option("path","Windowdata.csv")\
              .schema(windowdataSchema)\
              .load()

window_data.write.format("parquet")\
                .partitionBy("country","weeknum")\
                .option("path","assignment/q1_1")\
                .mode("Overwrite")\
                .save()

