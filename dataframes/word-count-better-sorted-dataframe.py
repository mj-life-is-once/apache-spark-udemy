from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# read each line of data into a dataframe
inputDF = spark.read.text("data/book.txt")

# split using a regular expression that extracts words
# all the words are in a column named "word"
words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
wordsWithoutEmptyString = words.filter(words.word != "")

print(wordsWithoutEmptyString.show())
# normalize everything to lowercase
lowercaseWords = wordsWithoutEmptyString.select(func.lower(wordsWithoutEmptyString.word).alias("word"))

# count up the occurrences of each word
wordCounts = lowercaseWords.groupBy("word").count()

# sort by counts
wordCountsSorted = wordCounts.sort("count")

# show the full results
wordCountsSorted.show(wordCountsSorted.count())