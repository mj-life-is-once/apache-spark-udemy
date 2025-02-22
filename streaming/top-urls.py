from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession

from pyspark.sql.functions import regexp_extract
from pyspark.sql import functions as func

# Create a spark session
spark = SparkSession.builder.appName("TopURLsSeenInLast30Seconds").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


# read in the streaming log data
accessLines = spark.readStream.text("logs")

# Parse out the common log format to a DataFrame
contentSizeExp = r"\s(\d+)$"
statusExp = r"\s(\d{3})\s"
generalExp = r"\"(\S+)\s(\S+)\s*(\S*)\""
timeExp = r"\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]"
hostExp = r"(^\S+\.[\S+\.]+\S+)\s"


logsDF = accessLines.select(
    regexp_extract("value", hostExp, 1).alias("host"),
    regexp_extract("value", timeExp, 1).alias("timestamp"),
    regexp_extract("value", generalExp, 1).alias("method"),
    regexp_extract("value", generalExp, 2).alias("endpoint"),
    regexp_extract("value", generalExp, 3).alias("protocol"),
    regexp_extract("value", statusExp, 1).cast("integer").alias("status"),
    regexp_extract("value", contentSizeExp, 1).cast("integer").alias("content_size"),
)

# the log is really old data from 2015, so we fabricate data as a current one
logsDF2 = logsDF.withColumn("eventTime", func.current_timestamp())

# window the data for the last 30 seconds
windowedCounts = (
    logsDF2.groupBy(
        func.window(func.col("eventTime"), "30 seconds", "10 seconds"),
        func.col("endpoint"),
    )
    .count()
    .orderBy(func.desc("count"))
)
# sorted EndPoint counts
sortedEndpointCounts = windowedCounts.sort("count", ascending=False)

# display the stream to the console
query = (
    sortedEndpointCounts.writeStream.outputMode("complete")
    .format("console")
    .queryName("counts")
    .start()
)

# wait until we terminate the scripts
query.awaitTermination()

# stop the session
spark.stop()
