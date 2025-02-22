import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    # normal python expression
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

input = sc.textFile("data/book.txt")
words = input.flatMap(normalizeWords)

# countbyValue() is not efficient for large datasets


# We want to keep use RDD to convert to each word to key/value pair with a value of 1
# Then count them all up with reduceBykey()
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

# Sort by count (value) in descending order
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey(ascending=True)
results = wordCountsSorted.collect()


for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)