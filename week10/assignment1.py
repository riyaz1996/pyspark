from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
sc = SparkContext("local[*]","parellize").getOrCreate()

#Exercise1: Find Number of Chapters Per Course


#chapter_courses_data = sc.textFile("D:/TrendyTech/week10/chapters-201108-004545.csv")
#
# chapter_courses = chapter_courses_data.map(lambda x : (int(x.split(",")[1]),int(1))).reduceByKey(lambda x,y : x+y)
#
# chapter_courses_list = chapter_courses.collect()
# for i in chapter_courses_list:
#     print(i)



viewsraw = sc.textFile("views*.csv").map(lambda x: (int(x.split(",")[1]),int(x.split(",")[0])))
viewsrawdistinct = viewsraw.distinct()
chapter_courses = sc.textFile("D:/TrendyTech/week10/chapters-201108-004545.csv").map(lambda x : (int(x.split(",")[0]),int(x.split(",")[1])))


chapter_courses_data2 = sc.textFile("D:/TrendyTech/week10/chapters-201108-004545.csv")

chapter_courses2 = chapter_courses_data2.map(lambda x : (int(x.split(",")[1]),int(1))).reduceByKey(lambda x,y : x+y)

joinedRDD = viewsrawdistinct.join (chapter_courses)

pairRDD = joinedRDD.map(lambda x : ( (x[1][0],x[1][1]) ,1) )

userPerCourseViewRDD = pairRDD.reduceByKey(lambda x,y:x+y)

courseViewsCountRDD = userPerCourseViewRDD.map(lambda x: (x[0][1],x[1]))

newJoinedRDD = courseViewsCountRDD.join(chapter_courses2)

CourseCompletionpercentRDD = newJoinedRDD.map(lambda x: (x[0],x[1][0]/x[1][1]))

scoresRDD = CourseCompletionpercentRDD.map(lambda x: (x[0], 101 if x[1] > 0.9 else (41 if x[1] >= 0.5 and x[1] < 0.9 else ((41 if x[1] >= 0.25 and x[1] < 0.5 else 1)))))

totalScorePerCourseRDD  = scoresRDD.reduceByKey(lambda x,y : x+y)

for i in totalScorePerCourseRDD.collect():
    print(i)


