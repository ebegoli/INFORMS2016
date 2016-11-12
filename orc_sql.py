
# Reference : https://bigdatatinos.com/2016/02/08/using-spark-hdinsight-to-analyze-us-air-traffic/

import pyspark
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
import atexit
from pyspark.sql import HiveContext

sc = SparkContext('local[*]')
sqlc = SQLContext(sc)
sqlHive= HiveContext(sc)
atexit.register(lambda: sc.stop())


import csv
from pyspark.sql.types import *

#==============
# ORC File operations
orcFile = sqlHive.read.orc('/Users/Shreeji/Desktop/SGNNN_INFORMS/Exp3/orc_data/delayedflights.orc')
orcFile.registerTempTable("orcFile");
#orcFile.printSchema();

#1 select avg delay from flights group by day
sqlHive.sql('SELECT DayOfWeek, AVG(DepDelayMinutes) AS Avg_Delay FROM orcFile GROUP BY DayOfWeek').show(100)

#2 select avg delay from flights group by destination
sqlHive.sql('SELECT DestCityName, AVG(DepDelayMinutes) AS Avg_Delay FROM orcFile GROUP BY DestCityName').show(100)

#3 select avg delay from flights group by destination and by month
sqlHive.sql('SELECT DestCityName, Month, AVG(DepDelayMinutes) AS Avg_Delay FROM orcFile GROUP BY DestCityName, Month ORDER BY DestCityName, Month').show(100)

#4 Total number of flight cancelled group by year and month
sqlHive.sql('SELECT Year, Month, COUNT(Cancelled) AS Total_Cancelled FROM orcFile WHERE Cancelled=1 GROUP BY Year, Month ORDER BY Year, Month').show(100)
#==============
