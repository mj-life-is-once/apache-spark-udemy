from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("data/fakefriends-header.csv")

print("Here is our inferred schema:")
people.printSchema()

print("Let's display the name column:")
people.select("name").show()

print("Make everyone 10 years older:")
people.select(people.name, people.age + 10).show()

print("Filter out anyone over 21")
people.filter(people.age < 21).show()

# only select age and friends column
friendsByAge = people.select("age", "friends")

print("Group by age")
friendsByAge.groupBy("age").count().show()

# average number of friend of that age
print("Group by age and compute average number of friends:")
friendsByAge.groupBy("age").agg({"friends": "avg"}).show()

# Sorted
friendsByAge.groupBy("age").agg({"friends": "avg"}).sort("age").show()

# formatted more nicely
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2)).sort("age").show()

# with a custome column name
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2).alias("friends_avg")).sort("age").show()
spark.stop()