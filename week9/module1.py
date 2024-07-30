from pyspark import SparkContext
from sys import stdin

if __name__ == "__main__":

    sc = SparkContext("local[*]", "wordcount")

    input = sc.textFile("file.txt")

    words = input.flatMap(lambda x : x.split(" "))

    word_counts = words.map(lambda x:(x,1))

    final_count = word_counts.reduceByKey(lambda x,y : x+y)

    result = final_count.collect()

    for i in result:
        print(i)

else:
    print("NA Executed")


stdin.readline() #holding the program