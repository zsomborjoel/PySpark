from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, column, desc, col


sc = SparkContext()
spark = SparkSession(sc)

staticDataFrame =  spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("retaildata.csv")

staticDataFrame.createOrReplaceTempView("retail_data")
staticSchema = staticDataFrame.schema

stremingDataFrame = spark.readStream\
    .schema(staticSchema)\
    .option("mayFilesPerTrigger", 1)\
    .format("csv")\
    .option("header", "true")\
    .load("retaildata.csv")

#Check if its stream able
val = stremingDataFrame.isStreaming

#setting up the business logic
purchaseByCustomerPerHour = stremingDataFrame\
    .selectExpr(
    "CustomerId",
    "(UnitPrice * Quantity) as total_cost",
    "InvoiceDate")\
    .groupBy(
    col("CustomerId"), window(col("InvoiceDate"), " 1 day"))\
    .sum("total_cost")

# optimalization to run this better on a single machine
spark.conf.set("spark.sql.shuffle.partitions", "5")

# start stream
purchaseByCustomerPerHour.writeStream\
    .format("memory")\
    .queryName("customer_purchases")\
    .outputMode("complete")\
    .start()

#query
spark.sql("""
    SELECT *
    FROM customer_purchases
    ORDER BY `sum(total_cost)` DESC
""")\
.show(5)

