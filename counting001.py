from pyspark.sql import SparkSession
from pyspark import SparkContext

sc = SparkContext()
spark = SparkSession(sc)

myRange = spark.range(1000).toDF("number")
division = myRange.where("number % 2 = 0")

count = division.count()

print(count)