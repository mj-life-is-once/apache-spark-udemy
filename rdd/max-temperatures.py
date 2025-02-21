from typing import Tuple
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MaxTemperatures")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

def parseLine(line: str) -> Tuple[str, str, float]:
    "return (stationId, entryType, temperature)"
    fields = line.split(",")
    stationId = fields[0]
    entryType = fields[2]
    # convert temperature from Fahrenheit to Celsius
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationId, entryType, temperature)

lines = sc.textFile("data/1800.csv")
parsedLines = lines.map(parseLine)

# calculate filter minimum temperature
minTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
# (stationId, temperature)
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
# get minimum temperature
minTemps = stationTemps.reduceByKey(lambda x, y: max(x, y))
results = minTemps.collect()

for result in results:
    print(f"{result[0]}, {round(result[1], 2)}F")