from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, current_timestamp, date_add, date_sub, datediff, months_between, to_date, lit, unix_timestamp, to_timestamp

sc = SparkContext()
spark = SparkSession(sc)

dateDF = spark.range(10)\
    .withColumn("today", current_date())\
    .withColumn("now", current_timestamp())

dateDF.createTempView("dateTable")

dateDF.printSchema()

#1 take days or add days
print("1")
dateDF.select(
    date_sub(col("today"), 5),
    date_add(col("today"), 5))\
.show(1)

#2 date diference
print("2")
dateDF\
    .withColumn("week_ago", date_sub(col("today"), 7))\
    .select(datediff(col("week_ago"), col("today")))\
    .show(1)

#3 between two month
print("3")
dateDF\
    .select(
    to_date(lit("2016-01-01")).alias("start"),
    to_date(lit("2017-05-22")).alias("end"))\
    .select(months_between(col("start"), col("end")))\
.show(1)

"""
SELECT
    to_date('2016-01-01'),
    month_between('2016-01-01', '2017-01-01'),
    datediff('2016-01-01', '2017-01-01')
FROM 
    dateTable
"""

#4 robust way fixing bad date format (which turns to null in spark)
print("4")
dateFormat= "yyyy-dd-MM"

cleanDateDF = spark.range(1)\
    .select(
    to_date(unix_timestamp(lit("2017-12-11"), dateFormat).cast("timestamp"))\
        .alias("date"),
    to_date(unix_timestamp(lit("2017-20-12"), dateFormat).cast("timestamp"))\
        .alias("date2"))

cleanDateDF.createOrReplaceTempView("dateTable2")

cleanDateDF.show()

"""
SELECT
    to_date(date, 'yyyy-dd-MM'),
    to_date(date2, 'yyyy-dd-MM'),
    to_date(date)
FROM 
    dateTable2
"""

#5 timestamp formatting
print("5")
cleanDateDF\
    .select(
        to_timestamp(col("date"), dateFormat))\
    .show()

#6 date comparison
print("6")
cleanDateDF.filter(col("date2") > lit("2017-12-12")).show()
