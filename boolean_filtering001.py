from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, instr, expr


sc = SparkContext()
spark = SparkSession(sc)

df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("retaildata.csv")

#1 gets only InvoiceNo and Description
print("1")
df.where(col("InvoiceNo") != 536365)\
    .select("InvoiceNo", "Description")\
    .show(5, False)

#2
print("2")
df.where("InvoiceNo = 536365")\
    .show(5, False)

#3
print("3")
df.where("InvoiceNo <> 536365")\
    .show(5, False)

#4
print("4")
priceFilter = col("UnitPrice") > 600
descriptionFilter = instr(df.Description, "WHITE") >= 1

df.where(df.StockCode.isin("DOT"))\
    .where(priceFilter | descriptionFilter)\
    .show()


""" =
SELECT *
FROM dfTable
WHERE StockCOde in ("DOT") 
AND (UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1)
"""


#5 Defines what is expensive
print("5")
DOTCodeFilter = col("StockCode") == "DOT"
priceFilter = col("UnitPrice") > 600
descriptionFilter = instr(col("Description"), "POSTAGE") >= 1

df.withColumn("isExpensive",
DOTCodeFilter & (priceFilter | descriptionFilter))\
    .where("isExpensive")\
    .select("unitPrice", "isExpensive")\
    .show(5)

""" =
SELECT 
    UnitPrice, 
    (StockCode = 'DOT' 
    AND (UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1)) as isExpensive
FROM dfTable
WHERE
    (StockCode = 'DOT' AND 
    (UnitPrice  > 600 OR
    instr(Description, "POSTAGE") >= 1))  
"""

#6
print("6")
df.withColumn("isExpensive", expr("NOT UnitPrice <= 250"))\
    .where("isExpensive")\
    .select("Description", "UnitPrice")\
    .show(5)

#7 Null handling
print("7")
df.where(col("Description").eqNullSafe("Hello")).show()