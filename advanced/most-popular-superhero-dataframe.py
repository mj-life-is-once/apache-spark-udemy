from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("id",IntegerType(), True),
    StructField("name", StringType(), True)
])

# read name from Marvel-names.txt
names = spark.read.schema(schema).option("sep", " ").csv("data/Marvel-names.txt")

# read lines from graph
lines = spark.read.csv("data/Marvel-graph.txt") # read every line as a string

# just calculate number of connections
connections = lines.withColumn("id", func.split(func.col("_c0"), " ")[0])\
    .withColumn("connections", func.size(func.split(func.col("_c0"), " ")) - 1)\
    .groupby("id").agg(func.sum("connections").alias("connections"))

# sort by connections in descending order
mostPopular = connections.sort("connections", ascending=False).first()

# find the name for the most popular superhero
# just show the first match
mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()

print(f"{mostPopularName[0]} is the most popular superhero with {mostPopular[1]} co-appearances.")