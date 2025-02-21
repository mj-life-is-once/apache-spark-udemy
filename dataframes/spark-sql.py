from pyspark.sql import SparkSession
from pyspark.sql import Row

# Create Spark Session
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

def mapper(line: str):
    fields = line.split(",")
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), \
               age=int(fields[2]), numFriends=int(fields[3]))

# Create Spark Context
# this is to show how RDD interacts with spark SQL
sc = spark.sparkContext
sc.setLogLevel("ERROR")

lines = sc.textFile("data/fakefriends.csv")

# this is going to create RDD
people = lines.map(mapper)

# Infer the schema, and register the DataFrame as a table.
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 and age <= 19")

# The result of SQL queries are RDDs and support all the normal RDD operations.
for teen in teenagers.collect():
    print(teen)

# can also use dataframe function instead of SQL queries
# this will show in tabular format
schemaPeople.groupBy("age").count().orderBy("age").show()

# stop the spark session
spark.stop()
