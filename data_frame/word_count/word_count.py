# Problem: From a given text file, find the total count of each word then save the results in a text file.

# following modules are used to run spark in local windows.
import findspark
findspark.init()
findspark.find()
import pyspark

from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("word_count").getOrCreate()
inputDF = spark.read.text("file:///santosh/code/repos/spark/data_frame/word_count/book.txt")
words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
words.filter(words.word!="")
lowercaseWords = words.select(func.lower(words.word).alias("word"))
wordCounts = lowercaseWords.groupBy("word").count()
wordCountsSorted = wordCounts.sort("count")
wordCountsSorted.show(wordCountsSorted.count())