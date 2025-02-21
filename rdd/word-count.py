import re
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

def normalizeWords(text):
    # break up the text into words
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

input = sc.textFile("data/book.txt")
# words = input.flatMap(lambda x: x.split())
words = input.flatMap(normalizeWords)

# only take the first 5 words

# this function counts the number of times each word appears in the RDD and returns a dictionary (not so efficient)
wordCounts = words.countByValue()
for word, count in list(wordCounts.items()):
    # ignore non-ascii characters
    cleanWord = word.encode("ascii", "ignore")
    if cleanWord:
        print(f"{cleanWord.decode('ascii')}: {count}")
