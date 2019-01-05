from pyspark.sql import SparkSession
from pyspark import SparkContext

sc = SparkContext()
spark = SparkSession(sc)

flightData2015 = spark\
    .read\
    .option("inferSchema", "true")\
    .option("header", "true")\
    .csv("flightData2015.csv")

data = flightData2015.take(3)

print(data)