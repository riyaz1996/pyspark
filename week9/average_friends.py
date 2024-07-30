from pyspark import SparkContext

sc = SparkContext("local[*]", "wordcount")

def parseline(x):
    fields = x.split("::")
    age  = int(fields[2])
    num_friends = int(fields[3])
    return(age,num_friends)


rdd1 = sc.textFile("friendsdata.csv")
rdd2 = rdd1.map(parseline)
rdd3 = rdd2.mapValues(lambda x:(x,1)).reduceByKey(lambda x,y:(x[0]+y[0], x[1]+y[1]))

rdd4 = rdd3.mapValues(lambda x: x[0]/x[1])

rdd4 = rdd4.collect()

for i in rdd4:
    print(i)