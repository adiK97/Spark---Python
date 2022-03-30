from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sc = SparkContext("local","Max review count")
spark = SparkSession(sc)


business = sc.textFile("business.csv").map(lambda line: line.split("::")).toDF(["Business_id", "Address", "Categories"]).distinct()
review = sc.textFile("review.csv").map(lambda line: line.split("::")).toDF(["Review_id", "User_id", "Business_id", "Rating"]).distinct()

merged_tables = business.join(review, "Business_id")

count_table = merged_tables.groupBy("Business_id").count().join(business, "Business_id")
res = count_table.select("Business_id", "Address", "Categories", "Count").orderBy(["Count"], ascending=False).limit(10)

res.show()
res.toPandas().to_csv("res-q4.csv", header=0, index=0, sep='\t')
sc.stop()