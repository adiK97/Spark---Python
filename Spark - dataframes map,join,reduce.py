import sys
 
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum

sc = SparkContext("local","q2")
spark = SparkSession(sc)
def jointList(x):
    temp = []
    for i in range(len(x[1])):
        first = min(x[0], x[1][i])
        second =  max(x[0], x[1][i])

        temp.append((str(first)+","+str(second), x[1]))
    return(temp)

friendList = sc.textFile("mutual.txt").map(lambda x: (x.split("\t")[0],  x.split("\t")[1].split(",")))
friendList = friendList.flatMap(jointList)

common = friendList.reduceByKey(lambda x,y:list(set(x).intersection(y)))
common = common.map(lambda x: (x[0] , len(x[1])))


test = common.toDF()
sum1 = test.agg(_sum("_2"))
count =  test.count()
# print(count)
# print(sum1.head()[0])
avg = test.agg(_sum("_2")).head()[0] / test.count()
# print(avg)

# print(common.take(3))
moreThanAvg = common.filter(lambda x: x[1] < avg)

# print(moreThanAvg.take(100))

moreThanAvg.saveAsTextFile("output/")

sc.stop()
