from pyspark import SparkContext

sc = SparkContext("local[*]", "wordcount")

rdd1 = sc.textFile("dataset1")

rdd2 = rdd1.map(lambda x : (x,'y' if int(x.split(',')[1])> 18 else 'N'))

rdd2 = rdd2.collect()

for i in rdd2:
    print(i)