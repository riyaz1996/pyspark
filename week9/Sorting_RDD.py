from pyspark import SparkContext

sc = SparkContext("local[*]", "wordcount")

input = sc.textFile("search_data.txt")

words = input.flatMap(lambda x : x.split(" "))

word_counts = words.map(lambda x:(x.lower(),1))

final_count = word_counts.reduceByKey(lambda x, y: x + y).map(lambda x: (x[1],x[0]) )
final_count = final_count.sortByKey(False)

final_result = final_count.collect()

for i in final_result:
    print(i)