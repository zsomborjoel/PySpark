from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, max, min, row_number, substring, concat, lit, regexp_replace, when, \
    current_date, to_timestamp, datediff
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Spark Context / Session
sc = SparkContext()
spark = SparkSession(sc)


# Read csv
df = spark.read.format("csv").option("header", "true").load("SalesJan2009.csv")

# Cast Price as Integer and Trim Product
df = df.withColumn("Price", col("Price").cast(IntegerType()))\
    .withColumn("Product", trim(col("Product")))

# Bank Kártyák Átlagát, és Summáját
# -------------------------------------------- Multiple aggregation ----------------------------------------------------
# [min, max] Create an expression to use later in the aggregate - this way multiple aggregations can be used
expr = [min(col("Price")), max(col("Price"))]
# * = asterisk is for unpacking in function call
aggr = df.groupBy("Product")\
    .agg(*expr)

# aggr.show()
# ------------------------------------------- Window function ----------------------------------------------------------
# [row_number] create specification what to partition and how to order in window function
window_spec = Window.partitionBy(col("Product"))\
                    .orderBy(col("Price").desc())

# than we choose the window function (in this case row_number) and add it to the dataframe
window_func = df.withColumn("Row_Number", row_number().over(window_spec))

# window_func.show()

# ------------------------------------------- String functions ---------------------------------------------------------
# [substring] get Product without number at the end, and add it to dataframe
# Kérjük le az Országok első 3 betüjét nagybetűvel
df_with_substr = df.withColumn("New_Product_Column", substring(col("Product"), 0, 7))
# df_with_substr.show()

# [concat] based on the previous substring we add "New_" string literal and overwrite New_Product column from prev df
df_with_concat = df_with_substr.withColumn("New_Product_Column", concat(lit("New_"), col("New_Product_Column")))
# df_with_concat.show()

# [replace] here we are using the base df and replace a country name
replaced_df = df.withColumn("Country", regexp_replace(col("Country"), "United States", "USA"))
# replaced_df.show()

# ------------------------------------------- CASE - WHEN ------------------------------------------------------------
# [case when] create new column with case when
df_with_case = df.withColumn("Case_When",
                        when(col("Price") < 1000, lit("Small"))\
                        .when(col("Price").between(1000, 1200), lit("Medium"))\
                        .when(col("Price").between(1201, 3600), lit("Big"))\
                        .when(col("Price") > 3600, lit("Huge"))\
                        .otherwise("Else")
                    )
#df_with_case.show()

# ------------------------------------------- Date functions ----------------------------------------------------------
# [current date]
df_curr_date = spark.range(1).select(current_date()).alias("Current_Date")

# [to timestamp] Used only two yy in "MM/dd/yy" because we don't have century in date
df_to_timestamp = df.withColumn("Account_Create", to_timestamp(col("Account_Created"), "MM/dd/yy HH:mm"))
# df_to_timestamp.show()
# [date difference]
df_date_diff = df_to_timestamp.select(datediff(current_date(), col("Account_Create"))).alias("Date_Difference")

# df_date_diff.show()
