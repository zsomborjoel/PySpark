from pyspark import SparkContext
from pyspark.sql import SparkSession

# Spark Context / Session
sc = SparkContext()
spark = SparkSession(sc)

example = sc.parallelize([(0, u'D'), (0, u'D'), (1, u'E'), (2, u'F')])

# x[0] = (0, 1, 2) | list(x[1]) = [['D', 'D'], ['E'], ['F']]
rdd = example.groupByKey().map(lambda x: (x[0], list(x[1])))


print(rdd.collect())

