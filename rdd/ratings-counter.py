from pyspark import SparkConf, SparkContext
import collections

# local : we're not doing any distributed computing,
#  we're just running on a single machine
# set log level to ERROR to avoid too much output
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

lines = sc.textFile("data/ml-100k/u.data")
# extract (map) the data we care about
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()
print(result)

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
