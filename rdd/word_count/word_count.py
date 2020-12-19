# Problem: From a given text file, find the total count of each word.

# following modules are used to run spark in local windows.
import findspark
findspark.init()
findspark.find()
import pyspark


# import required modules for Spark to execute. 
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

# map - transforms each element of an RDD into one new element.
# flatmap - ability to transofrm each element of an RDD to many new elements.

text_file = sc.textFile("file:///SparkCourse/book.txt")
words = text_file.flatMap(lambda x: x.split())
count_words = words.countByValue()

for word, count in count_words.items():
    print (word, count)