from pyspark.sql import SparkSession
from pyspark import SparkContext

sc = SparkContext()
spark = SparkSession(sc)

flightData2015 = spark\
    .read\
    .option("inferSchema", "true")\
    .option("header", "true")\
    .csv("flightData2015.csv")

#optimalization to run this better on a single machine
spark.conf.set("spark.sql.shuffle.partitions", "5")
flightData2015.sort("count").explain()