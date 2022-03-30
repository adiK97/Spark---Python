from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession


sc = SparkContext("local","Stanford reviews")
spark = SparkSession(sc)


user = sc.textFile("user.csv").map(lambda line: line.split("::")).toDF(["User_id", "Name", "URL"]).distinct()
business = sc.textFile("business.csv").map(lambda line: line.split("::")).toDF(["Business_id", "Address", "Categories"]).distinct()
review = sc.textFile("review.csv").map(lambda line: line.split("::")).toDF(["Review_id", "User_id", "Business_id", "Rating"]).distinct()

business_filtered = business.filter(business.Address.contains("Stanford"))
stanford_reviews = business_filtered.join(review, "Business_id")
userid_stanford_reviews = stanford_reviews.join(user, "User_id")

res = userid_stanford_reviews.select("name", "Rating")

res.toPandas().to_csv("res-5.csv", header=0, index=0, sep='\t')

sc.stop()
