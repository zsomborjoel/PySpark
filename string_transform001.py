from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import initcap, col, lower, upper, lpad, rpad, ltrim, rtrim, trim, lit


sc = SparkContext()
spark = SparkSession(sc)

df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("retaildata.csv")

#1 Makes first letters Uppercase
print("1")
df.select(initcap(col("Description"))).show()

""" = 
SELECT 
    initcap(Description)
FROM 
    dfTable
"""

#2 Uppercase and Lowercase functions
print("2")
df.select(
    col("Description"),
    lower(col("Description")),
    upper(lower(col("Description")))
)\
.show()

""" =
SELECT 
    Description,
    lower(Description),
    upper(lower(Description))
FROM 
    dfTable
"""

#3 Trim and Pad functions
print("3")
df.select(
    ltrim(lit(" HELLO ")).alias("ltrim"),
    rtrim(lit(" HELLO ")).alias("rtrim"),
    trim(lit(" HELLO ")).alias("trim"),
    lpad(lit("HELLO"), 3, " ").alias("lpad"),
    rpad(lit("HELLO"), 10, " ").alias("rpad")
)\
.show(2)

"""
SELECT 
    ltrim(" HELLO "),
    rtrim(" HELLO "),
    trim(" HELLO "),
    lpad("HELLO", 3, " "),
    rpad("HELLO", 3, " ")
FROM 
    dfTable
"""

#4