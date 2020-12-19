# Problem: From a given text file, find the total count of each word then save the results in a text file.

# following modules are used to run spark in local windows.
import findspark
findspark.init()
findspark.find()
import pyspark

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()
people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("fakefriends-header.csv")
    
print("inferred schema:")
people.printSchema()

print("display the name column values:")
people.select("name").show()

print("filter out anyone over 21:")
people.filter(people.age<21).show()

print("group by age:")
people.groupBy("age").count().show(100)

print("add 10 years to everyone's age:")
people.select(people.name, people.age+10).show()

spark.stop()