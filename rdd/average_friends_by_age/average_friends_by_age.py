# Problem: There is a file having fields as <id, name, age, num_of_friends). Write a spark program to find the averge number of friends for any given age.

# following modules are used to run spark in local windows.
import findspark
findspark.init()
findspark.find()
import pyspark

# import required modules for Spark to execuite. 
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("AvgNumOfFriendsByAge")
sc = SparkContext(conf=conf)

# following function wil extract the required fields from each row from the imported file in the RDD.
def parseRow(row):
    fields = row.split(',')
    age = int(fields[2])
    num_of_friends = int(fields[3])
    return (age, num_of_friends)

#get data from the file and store in a row RDD.
row = sc.textFile("C:/SparkCourse/fakefriends.csv")

# parse each row and extract the key value pair ithe age_friends RDD (age, number_of_friends)
age_friends = row.map(parseRow)

# for each row, first apply mapValues on each row and then reduceByKey
# mapValues - Pass each value in the key-value pair RDD through a map function without changing the keys; this also retains the original RDD’s partitioning.
# reduceByKey - reduceByKey(func, numPartitions=None, partitionFunc=<function portable_hash>)[source] 
#               Merge the values for each key using an associative and commutative reduce function.
#               This will also perform the merging locally on each mapper before sending results to a reducer, similarly to a “combiner” in MapReduce.
#               Output will be partitioned with numPartitions partitions, or the default parallelism level if numPartitions is not specified. Default partitioner is hash-partition.

totals_by_age = age_friends.mapValues(lambda x: (x,1)).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))
# Note: in the above example, reduceByKey will keep adding all the values of a particular key until all of the records pertaining to that key are processed.
#Important: reduceByKey is faster as it will not create new partitions.

average_by_age = totals_by_age.mapValues(lambda x: x[0] / x[1])
final_result = average_by_age.collect()
for result in final_result:
    print(result)

