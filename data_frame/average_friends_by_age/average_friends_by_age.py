# Problem: From a given text file, find the total count of each word then save the results in a text file.

# following modules are used to run spark in local windows.
import findspark
findspark.init()
findspark.find()
import pyspark


from pyspark.sql import SparkSession
from pyspark.sql import Row

def mapper(line):
    fields = line.split(',')
    return Row(id = int(fields[0]), name = str(fields[1].encode("utf-8")) , age = int(fields[2]) , num_friends = int(fields[3]))

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()
lines = spark.sparkContext.textFile("fakefriends.csv")
people = lines.map(mapper)
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

teenagers = spark.sql("Select * from people where age >=13 and age <=19")

for teen in teenagers.collect():
    print(teen)
spark.stop()

