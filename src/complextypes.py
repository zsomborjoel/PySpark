from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, split, size, array_contains, explode, create_map

#complex types are STRUCTS, ARRAYS, MAPS

sc = SparkContext()
spark = SparkSession(sc)

df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("retaildata.csv")

#1 Structs
print("1")
complexDF = df\
    .select((struct("Description", "InvoiceNo").alias("complex")))

complexDF.select(col("complex").getField("Description"))

#2 Arrays (split function creates an array)
print("2")
df.select(split(col("Description"), " ").alias("array_col"))\
    .selectExpr("array_col[0]")\
    .show(2)

#3 Array length
print("3")
df.select(size(split(col("Description"), " "))).show(2)

#4 Array Contains
print("4")
df.select(array_contains(split(col("Description"), " "), "WHITE")).show(2)

#5 Explode (takes a column and makes a row every value of the array)
print("5")
df.withColumn("splitted", split(col("Description"), " "))\
    .withColumn("exploded", explode(col("splitted")))\
    .select("Description", "InvoiceNo", "exploded")\
    .show(2)

#6 Maps
print("6")
df.select(create_map(col("Description"), col("InvoiceNo")).alias("complex_map"))\
    .show(2)


