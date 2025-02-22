from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("MostObscureSuperheros").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = StructType(
    [StructField("id", IntegerType(), True), StructField("name", StringType(), True)]
)

# read names from Marvel-names.txt
names = spark.read.schema(schema).option("sep", " ").csv("data/Marvel-names.txt")

# read graph data from Marvel-graph.txt as line
lines = spark.read.text("data/Marvel-graph.txt")

# read id column, read connections column and count the number of connections
connections = (
    lines.withColumn("id", func.split(func.col("value"), " ")[0])
    .withColumn("connections", func.size(func.split(func.col("value"), " ")) - 1)
    .groupBy("id")
    .agg(func.sum("connections").alias("connections"))
)

# caclulate min connections
minConnections = connections.agg(func.min("connections")).first()[0]

# filter out all the connections with minVal
mostObscureConnections = connections.filter(func.col("connections") == minConnections)
# print(mostObscureConnections.show())

# find name for the id
mostObscureNames = mostObscureConnections.join(names, "id")
# only take necessary columns
result = mostObscureNames.select("name").show()
