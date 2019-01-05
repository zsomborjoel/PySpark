from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col

sc = SparkContext()
spark = SparkSession(sc)

df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("retaildata.csv")

df.createTempView("dfTable")

#1 coalesce
print("1")
df.select(coalesce(col("Description"), col("CustomerId"))).show()

#2
# Nullif, (select secound value if the first is null)
# Ifnull, (allows return null if two values are equal)
# nvl, nvl2
print("2")

data = spark.sql(
    """
    SELECT 
        ifnull(null, 'return_value'),
        nullif('value', 'value'),
        nvl(null, 'return_value'),
        nvl2('not_null', 'return_value', "else_value") 
    FROM dfTable
    """
)

data.show(1)

#3 Drop null value ('', "any", "all")
print("3")
df.na.drop("any")
df.na.drop("all", subset=["StockCode", "InvoiceNo"])
""" = 
SELECT *
FROM dfTable
Where Decription IS NOT NULL
"""

#4 filling columns
print("4")
df.na.fill("All null values becomes this string")
df.na.fill("all", subset=["StockCode", "InvoiceNo"])

fill_cols_vals = {
    "StockCode": 5,
    "Description": "No Value"
}

df.na.fill(fill_cols_vals)

#5 replacing nulls
print("5")
df.na.replace([""], ["UNKNOWN"], "Description")

#6 Ordering
#asc_nulls_first, acs_nulls_last, desc....
