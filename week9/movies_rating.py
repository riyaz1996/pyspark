from pyspark import SparkContext

sc = SparkContext("local[*]", "wordcount")

rdd1 = sc.textFile("moviedata-201008-180523.data")

rdd2 = rdd1.map(lambda x: (x.split("\t")[2],1))

rdd3 = rdd2.reduceByKey(lambda x,y :x+y)

rdd3 =rdd3.collect()

for i in rdd3:
    print(i)