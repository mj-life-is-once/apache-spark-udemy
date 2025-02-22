from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("TotalSpentByCustomer").getOrCreate()
# minimize the log output
spark.sparkContext.setLogLevel("ERROR")

# generate schema
schema = StructType([
    StructField("customerId", IntegerType(), True),
    StructField("itemId", IntegerType(), True),
    StructField("price", FloatType(), True)
])

customerOrders = spark.read.schema(schema).csv("data/customer-orders.csv")

# only select customerId and price column
totalByCustomerSorted = customerOrders.groupBy("customerId").agg(func.round(func.sum("price"), 2).alias("total_spent")).sort("total_spent")

# show all the result
totalByCustomerSorted.show(totalByCustomerSorted.count())

spark.stop()