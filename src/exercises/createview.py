from pyspark.sql import SparkSession
from pyspark import SparkContext

sc = SparkContext()
spark = SparkSession(sc)

flightData2015 = spark\
    .read\
    .option("inferSchema", "true")\
    .option("header", "true")\
    .csv("flightData2015.csv")

flightData2015.createOrReplaceTempView("flight_data_2015")

sqlWay = spark.sql("""
    SELECT DEST_COUNTRY_NAME, count(1) as counter
    FROM flight_data_2015
    GROUP BY DEST_COUNTRY_NAME
""")

dataFrameWay = flightData2015\
    .groupBy("DEST_COUNTRY_NAME")\
    .count()

sqlWay.show()
dataFrameWay.show()