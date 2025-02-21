from typing import Tuple
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

# add function to parse the input data
def parseLine(line: str) -> Tuple[int, int]:
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("data/fakefriends.csv")
# create key-value pairs
rdd = lines.map(parseLine)

# mapValues() to convert the values to a tuple of (value, 1)
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averageByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
results = averageByAge.collect()
# iterate the result
for result in results:
    print(result)