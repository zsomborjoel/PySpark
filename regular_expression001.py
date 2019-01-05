from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, regexp_replace, col, translate, instr, expr, locate

sc = SparkContext()
spark = SparkSession(sc)

df = spark.read.format("csv")\
    .option("header", "True")\
    .option("inferSchema", "True")\
    .load("retaildata.csv")


#1 regex
print("1")
regex_string = "BLACK|WHITE|RED|GREEN|BLUE"

df.select(
    regexp_replace(col("Description"), regex_string, "COLOR")
    .alias("color_cleaned"),
    col("Description"))\
.show(2)

""" = 
SELECT 
    regexp_replace(Description, 'BLACK|WHITE|RED|GREEN|BLUE', 'COLOR') as color_cleaned,
    Description
FROM
    dfTable
"""

#2 replace characters with diferent characters
print("2")
df.select(
    translate(col("Description"), "LEET", "1327"),
    col("Description")
)\
.show(2)

"""
SELECT
    translate(Description, 'LEET', '1327'),
    Description
FROM
    dfTable
"""

#3 pulling out the first mentioned color
print("3")

extract_str = "(BLACK|WHITE|RED|GREEN|BLUE)"

df.select(
    regexp_extract(col("Description"), extract_str, 1)
    .alias("color_cleaned"),
    col("Description"))\
.show(2)

"""
SELECT
    regexp_extract(Description, '(BLACK|WHITE|RED|GREEN|BLUE)', 1),
    Description
FROM 
    dfTable
"""

#4 filter if it contains that string
print("4")
containsBlack = instr(col("Description"), "BLACK") >= 1
containsWhite = instr(col("Description"), "WHITE") >= 1

df.withColumn("hasSimpleColor", containsBlack | containsWhite)\
    .filter("hasSimpleColor")\
    .select("Description")\
    .show(3, False)

"""
SELECT 
    Description
FROM 
    dfTable
WHERE
    instr(Description, 'BLACK') >= 1 OR instr(Description, 'WHITE') >= 1
"""

#5 dynamic number of arguments
print("5")
simpleColors = ["black", "white", "red", "green", "blue"]

def color_locator(column, color_string):
    """
    This function creates a column declaring whether or
    not a given PySpark column contains the UPPERCASED
    color.
    Returns a new column type that can be used
    in a select statement.
    """
    c = ''
    return locate(color_string.upper(), column)\
        .cast("boolean")\
        .alias("is_" + c)

selectedColumns = [color_locator(df.Description, c) for c in simpleColors]
selectedColumns.append(expr("*"))

df\
    .select(*selectedColumns)\
    .where(expr("is_white OR is_red"))\
    .select("Description")\
    .show(3, False)

