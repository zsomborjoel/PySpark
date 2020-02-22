from pyspark import SparkContext
from pyspark.sql import SparkSession

# Spark Context / Session
sc = SparkContext()
spark = SparkSession(sc)

# Read csv
df = spark.read.format("csv").option("header", "true").load("SalesJan2009.csv")


# Filter (WHERE)
# df = df.where("Payment_Type = 'Visa'")


print('Action show')
df.show()

# print('Action count')
# df.count()
