from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs

def loadMovieNames():
    movieNames = {}
    with codecs.open("data/ml-100k/u.item", "r", encoding="ISO-8859-1", errors="ignore") as f:
        for line in f:
            fields = line.split("|")
            movieNames[int(fields[0])] = fields[1]
    return movieNames

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# broadcast the movie names to all the nodes
nameDict = spark.sparkContext.broadcast(loadMovieNames())

# Create schema when reading u.data
# userID, movieID, rating, timestamp
schema = StructType([
    StructField("userID", IntegerType(), True),
    StructField("movieID", IntegerType(), True),
    StructField("rating", IntegerType(), True), 
    StructField("timestamp", LongType(), True)
])

# load up movie data as dataframe
moviesDF = spark.read.option("sep", "\t").schema(schema).csv("data/ml-100k/u.data")

movieCounts = moviesDF.groupBy("movieID").count()

# Add name
# Create a user-defined function to look up movie names from our broadcasted dictionary
def lookupName(movieID: int):
    return nameDict.value[movieID]

# create a user defined function
lookupNameUDF = func.udf(lookupName)

# add a movieTitle column using our new udf
# create new column
moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(func.col("movieID")))

# sort the results
# use sort or orderBy
# sort is an alias for orderBy
sortedMoviesWithNames = moviesWithNames.sort("count", ascending=False)

# grab the top 10, do not truncate the movieTitle column
sortedMoviesWithNames.show(10, False)

# stop the session
spark.stop()