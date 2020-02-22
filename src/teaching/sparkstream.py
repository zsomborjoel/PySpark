from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

# Spark Context / Session
sc = SparkContext()
spark = SparkSession(sc)

# Limits partitions (and optimize stream)
spark.conf.set("spark.sql.shuffle.partitions", "5")

# Read csv
df = spark.read.format("csv").option("header", "true").load("C:/Users/gyurkovicszs/PycharmProjects/PoSpark/SalesRecords/1000SalesRecords.csv")

# Get data schema to parse future stream
staticSchema = df.schema

# staticSchema.show()

# Create stream
streamingDataFrame = spark.readStream\
                        .schema(staticSchema)\
                        .option("maxFilesPerTrigger", 1)\
                        .format("csv")\
                        .option("header", "true")\
                        .load("C:/Users/gyurkovicszs/PycharmProjects/PoSpark/SalesRecords/*.csv")

# Check if its streaming
print(streamingDataFrame.isStreaming)

# Aggregation
streamingDataFrame = streamingDataFrame.withColumn("Units Sold", streamingDataFrame["Units Sold"].cast(IntegerType()))
streamingDataFrame = streamingDataFrame\
                        .groupBy("Country")\
                        .sum("Units Sold")\
                        .withColumnRenamed("sum(Units Sold)", "Sum_units_sold")\
                        .orderBy("sum(Units Sold)", ascending=False)\
                        .limit(3)


# Start the stream (using complete outputMode to check whole dataframe) - append mode is an other option with watermarks
query = streamingDataFrame.writeStream \
            .outputMode("complete")\
            .format("console")\
            .start()

query.awaitTermination()
