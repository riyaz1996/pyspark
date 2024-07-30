from pyspark import SparkContext
sc = SparkContext("local[*]","KeyWordAmount")
sc.setLogLevel("ERROR")
def boring_words():
    boring_words_list = set(i.strip() for i in open("boringwords.txt"))
    return boring_words_list

initial_rdd = sc.textFile("bigdatacampaigndata.csv")

name_set = sc.broadcast(boring_words())


mapped_rdd = initial_rdd.map(lambda x : (float(x.split(",")[10]),x.split(",")[0]))

words = mapped_rdd.flatMapValues(lambda x : x.split(" "))

word_key = words.map(lambda x :(x[1].lower(),x[0] ))

word_key_filtered = word_key.filter(lambda x : x[0] not in name_set.value)

final_map = word_key_filtered.reduceByKey(lambda x,y: x+y).sortBy(lambda x:x[1],ascending=False).take(20)

for i in final_map:
    print(i)