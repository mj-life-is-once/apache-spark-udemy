from pyspark import SparkConf, SparkContext
from typing import Tuple

conf = SparkConf().setMaster("local").setAppName("TotalSpentByCustomer")
sc = SparkContext(conf = conf)
# minimize the log output
sc.setLogLevel("ERROR")

def parseLine(line: str) -> Tuple[str, float]:
    lines = line.split(",")
    customerId = int(lines[0])
    amount = float(lines[2])
    return (customerId, amount)

# read the file as rdd
input = sc.textFile("data/customer-orders.csv")
# parse the file
parsedLines = input.map(parseLine)

# this is not about counting number of occurences
# but rather summing up the amount spent by each customer
totalByCustomer = parsedLines.reduceByKey(lambda x, y: x + y)

# sort the result by the amount spent
results = totalByCustomer.map(lambda x: (x[1], x[0])).sortByKey().collect()
# only take the first 5 lines
for customerId, value in results:
    print(value, customerId)
