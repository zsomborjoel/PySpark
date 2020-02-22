import yaml
import os
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, when, quarter, year
from pyspark.sql.types import DecimalType, IntegerType


# get environment properties
path = os.path.dirname(os.path.abspath(__file__)) + '/conf/'  # directory of the script being run
filename = 'config.yaml'

with open(path + filename) as f:
    config = yaml.load(f.read(), Loader=yaml.FullLoader)  # it is a dictionary
env = config.get("dev")

# Spark Context / Session
sc = SparkContext()
spark = SparkSession(sc)
spark.conf.set("spark.sql.shuffle.partitions", "5")

# connection string (host, port, database name)
url = "jdbc:postgresql://{}:{}/{}".format(env.get("host"),
                                          env.get("port"),
                                          env.get("dbname"))

# read table
def getTable(tablename):
    jdbcDF = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", tablename) \
        .option("user", env.get("username")) \
        .option("password", env.get("password")) \
        .option("driver", config.get("driver")) \
        .load()
    return jdbcDF

# read SQL
def getSQL(sql):
    jdbcDF = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("query", sql) \
        .option("user", env.get("username")) \
        .option("password", env.get("password")) \
        .option("driver", config.get("driver")) \
        .load()
    return jdbcDF

# write data to postgres
def writeToCsv(df, csv_path):
    df.coalesce(1) \
        .write.format("com.databricks.spark.csv") \
        .option("header", "true") \
        .save(csv_path)

# write data to postgres
def writeToDb(df, table):
    df.write \
            .format("jdbc") \
            .option("url", url) \
            .option("dbtable", table) \
            .option("user", env.get("username")) \
            .option("password", env.get("password")) \
            .option("driver", config.get("driver")) \
            .save()



# ================================================================================================================
"""
-- Alphabetical List of Products
select distinct b.*, a.Category_Name
from Categories a 
inner join Products b on a.Category_ID = b.Category_ID
where b.Discontinued = 'N'
order by b.Product_Name;
"""
categoriesDF = getTable("david_test.categories")
productsDF = getTable("david_test.products")

productWithCategoryNameDF = categoriesDF.alias("c").join(productsDF.alias("p"), categoriesDF.category_id == productsDF.category_id, how="inner") \
    .filter("discontinued > 0") \
    .orderBy("product_name") \
    .dropDuplicates() \
    .select("p.*", "c.category_name")

# productWithCategoryNameDF.show()

# ------------------------------------------------------------------------------------------------------------
"""
-- Order Subtotals
select OrderID, 
    format(sum(UnitPrice * Quantity * (1 - Discount)), 2) as Subtotal
from order_details
group by OrderID
order by OrderID;
"""
orderDetailsDF = getTable("david_test.order_details")

subtotalByOrderIdDF = orderDetailsDF.groupBy("order_id") \
            .agg(sum(col("unit_price") * col("quantity") * (1 - col("discount"))).cast(DecimalType(10, 2)).alias("subtotal")) \
            .orderBy("order_id") \

# subtotalByOrderIdDF.show()

# ------------------------------------------------------------------------------------------------------------

"""
-- Ten Most Expensive Products
select * from
(
    select distinct ProductName as Ten_Most_Expensive_Products, 
           UnitPrice
    from Products
    order by UnitPrice desc
) as a
limit 10;
"""

topMostExpensiveProductDF = productsDF.select("product_name", "unit_price") \
                                        .orderBy("unit_price", ascending=False) \
                                        .dropDuplicates() \
                                        .limit(10)

# topMostExpensiveProductDF.show()

# ------------------------------------------------------------------------------------------------------------
"""
-- Quarterly Orders by Product
select a.ProductName, 
    d.CompanyName, 
    year(OrderDate) as OrderYear,
    format(sum(case quarter(c.OrderDate) when '1' 
        then b.UnitPrice*b.Quantity*(1-b.Discount) else 0 end), 0) "Qtr 1",
    format(sum(case quarter(c.OrderDate) when '2' 
        then b.UnitPrice*b.Quantity*(1-b.Discount) else 0 end), 0) "Qtr 2",
    format(sum(case quarter(c.OrderDate) when '3' 
        then b.UnitPrice*b.Quantity*(1-b.Discount) else 0 end), 0) "Qtr 3",
    format(sum(case quarter(c.OrderDate) when '4' 
        then b.UnitPrice*b.Quantity*(1-b.Discount) else 0 end), 0) "Qtr 4" 
from Products a 
inner join Order_Details b on a.ProductID = b.ProductID
inner join Orders c on c.OrderID = b.OrderID
inner join Customers d on d.CustomerID = c.CustomerID 
where c.OrderDate between date('1997-01-01') and date('1997-12-31')
group by a.ProductName, 
    d.CompanyName, 
    year(OrderDate)
order by a.ProductName, d.CompanyName;
"""

ordersDF = getTable("david_test.orders")
customersDF = getTable("david_test.Customers")

quarterlyOrdersByProductsDF = productsDF.alias("a").join(orderDetailsDF.alias("b"), productsDF.product_id == orderDetailsDF.product_id, how="inner") \
                                        .join(ordersDF.alias("c"), orderDetailsDF.order_id == ordersDF.order_id, how="inner") \
                                        .join(customersDF.alias("d"), ordersDF.customer_id == customersDF.customer_id, how="inner") \
                                        .filter(col("c.order_date").between("1997-01-01", "1997-12-31")) \
                                        .select("a.product_name", "d.company_name", year("c.order_date").alias("year_order_date"), "c.order_date", "b.unit_price", "b.quantity", "b.discount") \
                                        .groupBy("a.product_name", "d.company_name", "year_order_date") \
                                        .agg(sum(when(quarter("c.order_date") == 1, col("b.unit_price") * col("b.quantity") * (1 - col("b.discount"))).otherwise(0)).cast(IntegerType()).alias("qtr 1"),
                                             sum(when(quarter("c.order_date") == 2, col("b.unit_price") * col("b.quantity") * (1 - col("b.discount"))).otherwise(0)).cast(IntegerType()).alias("qtr 2"),
                                             sum(when(quarter("c.order_date") == 3, col("b.unit_price") * col("b.quantity") * (1 - col("b.discount"))).otherwise(0)).cast(IntegerType()).alias("qtr 3"),
                                             sum(when(quarter("c.order_date") == 4, col("b.unit_price") * col("b.quantity") * (1 - col("b.discount"))).otherwise(0)).cast(IntegerType()).alias("qtr 4")) \
                                        .select("a.product_name", "d.company_name", "year_order_date", "qtr 1", "qtr 2", "qtr 3", "qtr 4") \
                                        .orderBy("a.product_name", "d.company_name")


# quarterlyOrdersByProductsDF.show()

quarterlyOrdersByProductsDF.explain()