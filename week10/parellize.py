from pyspark import SparkContext
sc = SparkContext("local[*]","parellize")
sc.setLogLevel("ERROR")

lines = []
for i in open("D:/TrendyTech/week10/bigLog.txt"):
    lines.append(i)

print(len(lines))
print(lines[:5])
rdd = sc.parallelize(lines[:50000])

rdd_final = rdd.map(lambda x:(x.split(":")[0],1))

rdd_final1=rdd_final.reduceByKey(lambda x,y : x+y)

for i in rdd_final1.collect():
    print(i)
