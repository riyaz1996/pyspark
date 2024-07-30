from pyspark import SparkContext

sc = SparkContext("local[*]", "wordcount")

rdd1 = sc.textFile("customerorders.csv")

rdd2 = rdd1.map(lambda x: (x.split(",")[0],float(x.split(",")[2])))

rdd3 = rdd2.reduceByKey(lambda x,y :x + y).map(lambda x: (x[1],x[0]) )

rdd4 = rdd3.sortByKey(False)
rdd4 = rdd4.map(lambda x: (x[1],x[0]))

for i in rdd4.collect():
    print(i)