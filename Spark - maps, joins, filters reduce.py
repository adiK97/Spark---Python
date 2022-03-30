from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sc = SparkContext("local","Max review count")
spark = SparkSession(sc)


review = sc.textFile("review.csv").map(lambda line: line.split("::")).toDF(["Review_id", "User_id", "Business_id", "Rating"]).distinct()
user = sc.textFile("user.csv").map(lambda line: line.split("::")).toDF(["User_id", "Name", "URL"]).distinct()

total_reviews = review.count()
print(total_reviews)

contribution = review.groupBy("User_id").count().join(user, "User_id")
contribution = contribution.withColumn("Count", F.col("Count")*100 / total_reviews)
res = contribution.select("Name", "Count").orderBy(["Count"], ascending=False).limit(10)

# print(res.agg(F.sum("Count")).collect()[0][0])
# res.show()
res.toPandas().to_csv("res-q5.csv", header=0, index=0, sep='\t')
sc.stop()