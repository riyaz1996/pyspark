from pyspark import SparkContext
sc = SparkContext("local[*]","Accumulator")
sc.setLogLevel("ERROR")


def blankline(line):
   # global myaccum
    if len(line)==0:
        myaccum.add(1)

myrdd = sc.textFile("sample_file.txt")

myaccum = sc.accumulator(0)

#foreach can be used on a rdd not on a python local variable
myrdd.foreach(blankline)

print(myaccum.value)