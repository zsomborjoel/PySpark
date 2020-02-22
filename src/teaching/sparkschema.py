from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, Row

sc = SparkContext()
spark = SparkSession(sc)

# Hogyan kell séma nélküli dataframe-nek sémát készíteni
# ------------------------------------------------------
"""
Spark adat típusok:
- BooleanType
- StringType
- BinaryType
- ByteType
- CalendarIntervalType
- DateType
- TimestampType
- DoubleType
- FloatType
- ShortType
- IntegerType
- LongType
- NumericType
- NullType
"""

# StructField paraméterei
#  StructField(String name,
#              DataType dataType,
#              boolean nullable,
#              Metadata metadata)

# Séma létrehozása hogy fog kinézni a dataframe aminek nincs sémája
schema = StructType([
    StructField("gyümölcs", StringType(), False),
    StructField("ár", IntegerType(), False),
    StructField("súly", IntegerType(), False),
    StructField("aktív", StringType(), True)
])

createdDF = spark.createDataFrame([Row("Alma", 200, 10, "A"),
                                    Row("Körte", 150, 12, ""),
                                    Row("Barack", 400, 25, None),
                                    Row("Banán", 300, 20, "I")
                                   ], schema)

createdDF.show()