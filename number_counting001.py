from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, pow, col, lit, round, bround, corr, monotonically_increasing_id


sc = SparkContext()
spark = SparkSession(sc)

df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("retaildata.csv")


#1 fabricate calculation in variable
print("1")
fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5

df.select(
    expr("CustomerId"),
    fabricatedQuantity.alias("realQuantity"))\
.show(2)

#2 fabricate calculation in select
print("2")
df.selectExpr(
    "CustomerId",
    "(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity")\
.show(2)

""" =
SELECT 
    customerId,
    (POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity
FROM dfTable
"""

#3 rounding numbers (bround will round down)
print("3")
df.select(
    round(lit("2.5")),
    bround(lit("2.5"))
)\
.show(2)

""" =
SELECT 
    round(2.5),
    bround(2.5)
"""

#4 Statistical Correlation between Quantity and UnitPrice
print("4")
df.stat.corr("Quantity", "UnitPrice")
df.select(corr("Quantity", "UnitPrice")).show()

"""
SELECT
    corr(Quantity, UnitPrice)
FROM 
    dfTable
"""

#5 Describe function show (count, mean, standard derivcation, min, max)
print("5")
df.describe().show()

#6 Other Stat function example
print("6")
colName = "UnitPrice"
quantileProbs = [0.5]
relError = 0.05
df.stat.approxQuantile("UnitPrice", quantileProbs, relError)

#7 Cross tabiliation, frequent item pairs
print("7")
df.stat.crosstab("StockCode", "Quantity").show()
df.stat.freqItems(["StockCode", "Quantity"]).show()

#8 Generating unique_id
print("8")
df.select(monotonically_increasing_id()).show()
df.select(monotonically_increasing_id() + 1).show()