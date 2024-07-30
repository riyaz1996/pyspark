from pyspark import SparkContext
sc = SparkContext("local[*]","parellize")
sc.setLogLevel("ERROR")

lines = sc.textFile("bigLog.txt")


rdd_final = lines.map(lambda x:(x.split(":")[0],1))

rdd_final1=rdd_final.reduceByKey(lambda x,y : x+y)

for i in rdd_final1.collect():
    print(i)
