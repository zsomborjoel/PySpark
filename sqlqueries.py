from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import max, desc

sc = SparkContext()
spark = SparkSession(sc)

flightData2015 = spark\
    .read\
    .option("inferSchema", "true")\
    .option("header", "true")\
    .csv("flightData2015.csv")

flightData2015.createOrReplaceTempView("flight_data_2015")


sqlway01 = spark.sql("SELECT max(count) FROM flight_data_2015").take(1)

dataframeway01 = flightData2015.select(max("count")).take(1)

sqlway02 = spark.sql("""
    SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
    FROM flight_data_2015
    GROUP BY DEST_COUNTRY_NAME
    ORDER BY sum(count) DESC
    LIMIT 5
""")

sqlway02.show()

dataframeway01 = flightData2015\
    .groupBy("DEST_COUNTRY_NAME")\
    .sum("count")\
    .withColumnRenamed("sum(count)", "destination_total")\
    .sort(desc("destination_total"))\
    .limit(5)\
    .explain()

