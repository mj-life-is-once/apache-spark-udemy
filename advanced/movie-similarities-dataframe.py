from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import sys


def computeCosineSimilarity(data):
    # compute cosine similarity
    pairScores = (
        data.withColumn("xx", func.col("rating1") * func.col("rating1"))
        .withColumn("yy", func.col("rating2") * func.col("rating2"))
        .withColumn("xy", func.col("rating1") * func.col("rating2"))
    )

    # compute numerator, denominator and numPairs columns
    calculateSimilarity = pairScores.groupBy("movie1", "movie2").agg(
        func.sum(func.col("xy")).alias("numerator"),
        (
            func.sqrt(func.sum(func.col("xx"))) * func.sqrt(func.sum(func.col("yy")))
        ).alias("denominator"),
        func.count(func.col("xy")).alias("numPairs"),
    )

    # calculate score
    result = calculateSimilarity.withColumn(
        "score",
        func.when(
            func.col("denominator") != 0,
            func.col("numerator") / func.col("denominator"),
        ).otherwise(0),
    ).select("movie1", "movie2", "score", "numPairs")

    return result


# Get movie name by given movie id
def getMovieName(movieNames, movieId):
    result = (
        movieNames.filter(func.col("movieID") == movieId)
        .select("movieTitle")
        .collect()[0]
    )

    return result[0]


spark = (
    SparkSession.builder.appName("MovieSimilarities").master("local[*]").getOrCreate()
)

movieNamesSchema = StructType(
    [
        StructField("movieID", IntegerType(), True),
        StructField("movieTitle", StringType(), True),
    ]
)

moviesSchema = StructType(
    [
        StructField("userID", IntegerType(), True),
        StructField("movieID", IntegerType(), True),
        StructField("rating", IntegerType(), True),
        StructField("timestamp", LongType(), True),
    ]
)


# I want to use every cpu core on my machine to process the data
# not really recommended for production code
spark = (
    SparkSession.builder.appName("MovieSimilarities").master("local[*]").getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

movieNameSchema = StructType(
    [
        StructField("movieID", IntegerType(), True),
        StructField("movieTitle", StringType(), True),
    ]
)

movieSchema = StructType(
    [
        StructField("userID", IntegerType(), True),
        StructField("movieID", IntegerType(), True),
        StructField("rating", IntegerType(), True),
        StructField("timestamp", LongType(), True),
    ]
)

# read movie names

movieNames = (
    spark.read.option("sep", "|")
    .option("charset", "ISO-8859-1")
    .schema(movieNameSchema)
    .csv("data/ml-100k/u.item")
)


# read movie data
movies = spark.read.option("sep", "\t").schema(movieSchema).csv("data/ml-100k/u.data")

# only select necessary columns
ratings = movies.select("userID", "movieID", "rating")

# create pairs
# Emit every movie rated together by the same user.
# Self-join to find every combination.
# Select movie pairs and rating pairs
moviePairs = (
    ratings.alias("ratings1")
    .join(
        ratings.alias("ratings2"),
        (func.col("ratings1.userID") == func.col("ratings2.userID"))
        & (func.col("ratings1.movieID") < func.col("ratings2.movieID")),
    )
    .select(
        func.col("ratings1.movieID").alias("movie1"),
        func.col("ratings2.movieID").alias("movie2"),
        func.col("ratings1.rating").alias("rating1"),
        func.col("ratings2.rating").alias("rating2"),
    )
)

# compute cosine similarity, and cache value
moviePairSimilarities = computeCosineSimilarity(moviePairs).cache()


if len(sys.argv) > 1:
    scoreThreshold = 0.97
    coOccurrenceThreshold = 50.0

    movieID = int(sys.argv[1])

    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    filteredResults = moviePairSimilarities.filter(
        ((func.col("movie1") == movieID) | (func.col("movie2") == movieID))
        & (func.col("score") > scoreThreshold)
        & (func.col("numPairs") > coOccurrenceThreshold)
    )

    # Sort by quality score.
    results = filteredResults.sort(func.col("score").desc()).take(10)

    print("Top 10 similar movies for " + getMovieName(movieNames, movieID))

    for result in results:
        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = result.movie1
        if similarMovieID == movieID:
            similarMovieID = result.movie2

        print(
            getMovieName(movieNames, similarMovieID)
            + "\tscore: "
            + str(result.score)
            + "\tstrength: "
            + str(result.numPairs)
        )
