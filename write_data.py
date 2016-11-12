
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
from StringIO import StringIO
# A helper function for getting the CSV values in a string
def RepresentsInt(s):
    try:
        int(s)
        return int(s)

    except ValueError:
        return int(0)

def RepresentsFlt(s):
    try:
        float(s)
        return float(s)
    except ValueError:
        return float(0.0)

def csv_values_in_line(line):
    sio = StringIO(line)
    value = csv.reader(sio).next()
    sio.close()
    return value


## Read all csv files from "input_csv_data" directory
csvFile = sc.textFile('/Users/Shreeji/Desktop/SGNNN_INFORMS/Exp3/input_csv_data/*.csv').map(csv_values_in_line).map(lambda r: (r[0],r[1],r[2],r[3],r[4],r[5]
                ,r[6],r[7],r[8],r[11],r[14],r[15],r[16],r[18]
                ,r[20],r[23],r[24],r[25],r[27],RepresentsFlt(r[32]),RepresentsFlt(r[34]),
                RepresentsFlt(r[43]),RepresentsFlt(r[45]),RepresentsFlt(r[47])
                ,RepresentsFlt(r[56]),RepresentsFlt(r[57]),RepresentsFlt(r[58]),RepresentsFlt(r[59])
                ,RepresentsFlt(r[60]),RepresentsFlt(r[32])))


from pyspark.sql.types import *
schema =  StructType([StructField('Year',StringType(), True),
StructField('Quarter',StringType(), True),StructField('Month',StringType(), True),
StructField('DayofMonth',StringType(), True),StructField('DayOfWeek',StringType(), True),
StructField('FlightDate',StringType(), True),StructField('UniqueCarrier',StringType(), True),
StructField('AirlineID',StringType(), True),StructField('Carrier',StringType(), True),
StructField('OriginAirportID',StringType(), True),StructField('Origin',StringType(), True),
StructField('OriginCityName',StringType(), True),StructField('OriginState',StringType(), True),
StructField('OriginStateName',StringType(), True),StructField('DestAirportID',StringType(), True),
StructField('Dest',StringType(), True),StructField('DestCityName',StringType(), True),
StructField('DestState',StringType(), True),StructField('DestStateName',StringType(), True),
StructField('DepDelayMinutes' , FloatType(), True),
StructField('DepartureDelayGroups', FloatType(), True),StructField('ArrDelayMinutes' , FloatType(), True),
StructField('ArrivalDelayGroups'  , FloatType(), True),
StructField('Cancelled' , FloatType(), True),
StructField('CarrierDelay' , FloatType(), True),StructField('WeatherDelay' , FloatType(), True),
StructField('NASDelay' , FloatType(), True),StructField('SecurityDelay' , FloatType(), True),
StructField('LateAircraftDelay' , FloatType(), True), StructField('DepDelayMinutesstr' , FloatType(), True)])

df = sqlc.createDataFrame(csvFile , schema)
dfHive = sqlHive.createDataFrame(csvFile , schema)


#df.printSchema()
#df.count()

## Generate Parquet format files using DataFrame
#df.write.parquet('wasb:///parqall/delayedflights.parquet')
df.write.parquet('/Users/Shreeji/Desktop/SGNNN_INFORMS/Exp3/parquet_data/delayedflights.parquet')



## Generate ORC format files for that you need to have Hive Context 
#from pyspark.sql import HiveContext
#sqlHive= HiveContext(sc)
dfHive.write.orc('/Users/Shreeji/Desktop/SGNNN_INFORMS/Exp3/orc_data/delayedflights.orc')



## Register data frame using CSV format
#==============
df.registerTempTable('csvflperf')

      #sqlc.sql('SELECT *  FROM csvflperf where DepDelayMinutes >15 '  ).show(100)

#1 select avg delay from flights group by day
sqlc.sql('SELECT DayOfWeek, AVG(DepDelayMinutes) AS Avg_Delay FROM csvflperf GROUP BY DayOfWeek').show(100)

#2 select avg delay from flights group by destination
sqlc.sql('SELECT DestCityName, AVG(DepDelayMinutes) AS Avg_Delay FROM csvflperf GROUP BY DestCityName').show(100)

#3 select avg delay from flights group by destination and by month
sqlc.sql('SELECT DestCityName, Month, AVG(DepDelayMinutes) AS Avg_Delay FROM csvflperf GROUP BY DestCityName, Month ORDER BY DestCityName, Month').show(100)

#4 Total number of flight cancelled group by year and month
sqlc.sql('SELECT Year, Month, COUNT(Cancelled) AS Total_Cancelled FROM csvflperf WHERE Cancelled=1 GROUP BY Year, Month ORDER BY Year, Month').show(100)
#==============


