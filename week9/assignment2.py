from pyspark import SparkContext

sc = SparkContext("local[*]", "wordcount")

def parseline(line):
    a = line.split(",")
    station_id = a[0]
    entry_type = a[2]
    temprature = a[3]
    return (station_id,entry_type,temprature)

rdd1 = sc.textFile("tempdata.csv")
rdd2 = rdd1.map(parseline)
rdd3 = rdd2.filter(lambda x : x[1] =='TMIN').map(lambda x : (x[0],x[2]))
rdd4 = rdd3.reduceByKey(lambda x,y : min(x,y))
rdd4 = rdd4.map(lambda x : (x[0],str(x[1])+'F'))



for i in rdd4.collect():
    print(i)




