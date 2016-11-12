
# Reference : https://bigdatatinos.com/2016/02/08/using-spark-hdinsight-to-analyze-us-air-traffic/

import pyspark
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
import atexit

sc = SparkContext('local[*]')
sqlc = SQLContext(sc)
atexit.register(lambda: sc.stop())

import csv
from pyspark.sql.types import *

#==============
# parquetFile operations
parquetFile = sqlc.read.parquet('/Users/Shreeji/Desktop/SGNNN_INFORMS/Exp3/parquet_data/delayedflights.parquet')
parquetFile.registerTempTable("parquetFile");
#parquetFile.printSchema();

#1 select avg delay from flights group by day
sqlc.sql('SELECT DayOfWeek, AVG(DepDelayMinutes) AS Avg_Delay FROM parquetFile GROUP BY DayOfWeek').show(100)

#2 select avg delay from flights group by destination
sqlc.sql('SELECT DestCityName, AVG(DepDelayMinutes) AS Avg_Delay FROM parquetFile GROUP BY DestCityName').show(100)

#3 select avg delay from flights group by destination and by month
sqlc.sql('SELECT DestCityName, Month, AVG(DepDelayMinutes) AS Avg_Delay FROM parquetFile GROUP BY DestCityName, Month ORDER BY DestCityName, Month').show(100)

#4 Total number of flight cancelled group by year and month
sqlc.sql('SELECT Year, Month, COUNT(Cancelled) AS Total_Cancelled FROM parquetFile WHERE Cancelled=1 GROUP BY Year, Month ORDER BY Year, Month').show(100)
#==============
