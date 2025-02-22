from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("FriendsByAge").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
# provide schema
schema = StructType([ StructField("stationId", StringType(), True),
                      StructField("date", IntegerType(), True),
                      StructField("measure_type", StringType(), True),
                      StructField("temperature", FloatType(), True)])


# set log level to Error
spark.sparkContext.setLogLevel("ERROR")

# read the file as dataframe
df = spark.read.schema(schema).csv("data/1800.csv")
df.printSchema()

# only take TMIN
df.filter(df.measure_type == "TIMIN")

# select only stationID and temperature
stationTemps = df.select("stationId", "temperature")

# aggregate to find min temperate for every station
minTempsByStation = stationTemps.groupBy("stationId").min("temperature")
minTempsByStation.show()

# convert the temp to farenheit and sort the dataset
minTempsByStationF = minTempsByStation.withColumn("temperature", 
                                                  func.round(func.col("min(temperature)") * 0.1 * (9.0 / 5.0) + 32.0, 2))\
                                                  .select("stationId", "temperature").sort("temperature")


# collect, format and print the results
results = minTempsByStationF.collect()
for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))

spark.stop()


