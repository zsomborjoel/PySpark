from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext()
spark = SparkSession(sc)

df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("retaildata.csv")

#getting schema like sysibm.syscolumns
df.printSchema()

df.createOrReplaceTempView("dfTable")

