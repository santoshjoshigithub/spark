# Problem: 1800.csv is a comma seprated file having four columns <weather_station, date, indicator, temprature>, 
# write a spark program to find the minimum temprature observed for each weather station.

# following modules are used to run spark in local windows.
import findspark
findspark.init()
findspark.find()
import pyspark

# import required modules for Spark execution.
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("MinTemp")
sc = SparkContext(conf=conf)

def parseRow(row):
    fields = row.split(',')
    station_id = fields[0]
    entry_type = fields[2]
    temp_deg_celsium = float(fields[3]) * 0.1 * (9.0/5.0) + 32.0
    return (station_id, entry_type, temp_deg_celsium)

#get data from the file and store in a row RDD.
row = sc.textFile("C:/SparkCourse/1800.csv")#change the path as per the file location.
station_entry_temp = row.map(parseRow)#this will parse each row of the csv file.
station_with_min_temp = station_entry_temp.filter(lambda x :"TMIN" in x[1])#filter only the min temp.
station_temp = station_with_min_temp.map(lambda x : (x[0], x[2]))#dropped the unwanted field x[1]
min_station_temp = station_temp.reduceByKey(lambda x,y : min(x,y))#find min based on each key.

final = min_station_temp.collect()

for result in final:
    print (result[0] + "\t{:.2f}F".format(result[1]))#formatted the output.