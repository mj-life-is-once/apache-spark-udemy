from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


# Create schema when reading u.data
# userID, movieID, rating, timestamp
schema = StructType([
    StructField("userID", IntegerType(), True),
    StructField("movieID", IntegerType(), True),
    StructField("rating", IntegerType(), True), 
    StructField("timestamp", LongType(), True)
])

# Load up movie data as dataframe
movieDF = spark.read.option("sep", "\t").schema(schema).csv("data/ml-100k/u.data")

# print(movieDF.show(5))
# Sort all movies by popularity by descending order
topMovieIDs = movieDF.groupBy("movieId").count().orderBy("count", ascending=False)
# topMovieIDs = movieDF.groupBy("movieId").count().orderBy(func.desc("count"))
topMovieIDs.show(10)

# stop the session
spark.stop()