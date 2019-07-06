from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, column,desc, col

sc = SparkContext()
spark = SparkSession(sc)

staticDataFrame =  spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("retaildata.csv")

staticDataFrame.createOrReplaceTempView("retail_data")
staticSchema = staticDataFrame.schema


staticDataFrame\
    .selectExpr(
    "CustomerId",
    "(UnitPrice * Quantity) as total_cost",
    "InvoiceDate")\
    .groupBy(
    col("CustomerId"), window(col("InvoiceDate"), "1 day"))\
    .sum("total_cost")\

