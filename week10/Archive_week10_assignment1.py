from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
sc = SparkContext("local[*]","parellize").getOrCreate()

spark = SparkSession.builder \
    .appName("Read CSV File") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

sc.setLogLevel("ERROR")

file_path = "D:/TrendyTech/week10/chapters-201108-004545.csv"
header_file_path = "D:/TrendyTech/week10/chaptersheader-201108-004545.csv"
data_file_path = "D:/TrendyTech/week10/chapters-201108-004545.csv"




# Read the header names from the header file
header_names = open(header_file_path).read().strip().split(',')

fields = [StructField(header, StringType(), True) for header in header_names]
schema = StructType(fields)



chapters_df = spark.read.csv(data_file_path, header=False,schema=schema, inferSchema=True)

chapters_df.show()
