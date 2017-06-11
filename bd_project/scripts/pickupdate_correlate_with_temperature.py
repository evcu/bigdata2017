from __future__ import print_function

import sys
import numpy
import subprocess
from pyspark.mllib.stat import Statistics
from csv import reader
from operator import add
from validation_utils import *
from myUtils import *
from pyspark import SparkContext

sc = SparkContext()
sc.addPyFile("myUtils.py")
sc.addPyFile("validation_utils.py")

def replaceValue(given_value, missing_value, replacement_value):
    if given_value == missing_value:
        return replacement_value
    return given_value

def calculateCorrelation(rdd1, rdd2):
    joined_rdd = rdd1.join(rdd2).sortByKey()

    rdd1_values = joined_rdd.map(lambda x:x[1][0])
    rdd2_values = joined_rdd.map(lambda x:x[1][1])
    correlation_value = Statistics.corr(rdd1_values, rdd2_values)
    return (joined_rdd,correlation_value)

 
	#(taxi_data,prefix) = readFiles(['data/yellow_tripdata_2016-01.csv','data/yellow_tripdata_2016-02.csv','data/yellow_tripdata_2016-03.csv','data/yellow_tripdata_2016-04.csv','data/yellow_tripdata_2016-05.csv','data/yellow_tripdata_2016-06.csv'],sc )
filenames = getAllFileNames()
(taxi_data_1,prefix) = readAllFiles(sc)

weather_data_temperature = readWeatherData(sc).map(lambda x: (datetime.datetime.strptime(x[0], '%Y%m%d'),replaceValue(float(x[1]),9999.9, 0))).filter(lambda x: x[0].year < 2017).map(lambda x: (str(x[0].date()),x[1]))
taxi_data = taxi_data_1.map(lambda entry: (str(datetime.datetime.strptime(entry[1], '%Y-%m-%d %H:%M:%S').date()),checkPickUpDateValid(entry[1]))).filter(lambda x: x[1] == "Valid").map(lambda x: (x[0],1)).reduceByKey(lambda x,y: x+y)

(joined_weather_taxi_trips,correlation_value_with_temperature) =calculateCorrelation(taxi_data,weather_data_temperature)

tabSeparated =  joined_weather_taxi_trips.map(lambda x: str(x[0])+"\t"+str(x[1][0])+"\t"+str(x[1][1])) 

#tabSeparated.saveAsTextFile("pickup_date_weather.out")
#taxi_data_values = joined_weather_taxi_trips.map(lambda x:x[1][0])
#weather_data_values = joined_weather_taxi_trips.map(lambda x:x[1][1])

#correlation_value_with_temperature= Statistics.corr(taxi_data_values, weather_data_values) 


########################### Windspeed ####################################
weather_data = readWeatherData(sc).map(lambda x: (datetime.datetime.strptime(x[0], '%Y%m%d'),replaceValue(float(x[final_weather_fieldsDic['windspeed']]),999.9,0))).filter(lambda x: x[0].year < 2017).map(lambda x: (str(x[0].date()),x[1]))

#joined_weather_taxi_trips =taxi_data.join(weather_data)

#taxi_data_values = joined_weather_taxi_trips.map(lambda x:x[1][0])
#weather_data_values = joined_weather_taxi_trips.map(lambda x:x[1][1])

#correlation_value_with_windspeed= Statistics.corr(taxi_data_values, weather_data_values)

(joined_weather_taxi_trips,correlation_value_with_windspeed) =calculateCorrelation(taxi_data,weather_data)
#tabSeparated =  joined_weather_taxi_trips.map(lambda x: str(x[0])+"\t"+str(x[1][0])+"\t"+str(x[1][1]))

#tabSeparated.saveAsTextFile("pickup_date_windspeed.out")


######################### Precipitation ###################################

weather_data = readWeatherData(sc).map(lambda x: (datetime.datetime.strptime(x[0], '%Y%m%d'),replaceValue(float(x[final_weather_fieldsDic['precipitation']][:-1]),99.99,0))).filter(lambda x: x[0].year < 2017).map(lambda x: (str(x[0].date()),x[1]))

(joined_weather_taxi_trips,correlation_value_with_precipitation) =calculateCorrelation(taxi_data,weather_data)

#joined_weather_taxi_trips =taxi_data.join(weather_data)
#taxi_data_values = joined_weather_taxi_trips.map(lambda x:x[1][0])
#weather_data_values = joined_weather_taxi_trips.map(lambda x:x[1][1])

#correlation_value_with_precipitation = Statistics.corr(taxi_data_values, weather_data_values)

tabSeparated =  joined_weather_taxi_trips.map(lambda x: str(x[0])+"\t"+str(x[1][0])+"\t"+str(x[1][1]))

#tabSeparated.saveAsTextFile("pickup_date_precipitation.out")


######################### Snow Depth #####################################
weather_data = readWeatherData(sc).map(lambda x: (datetime.datetime.strptime(x[0], '%Y%m%d'),replaceValue(float(x[final_weather_fieldsDic['snow_depth']]),999.9,0))).filter(lambda x: x[0].year < 2017).map(lambda x: (str(x[0].date()),x[1]))


(joined_weather_taxi_trips,correlation_value_with_snow_depth) =calculateCorrelation(taxi_data,weather_data)
#joined_weather_taxi_trips =taxi_data.join(weather_data)

#taxi_data_values = joined_weather_taxi_trips.map(lambda x:x[1][0])
#weather_data_values = joined_weather_taxi_trips.map(lambda x:x[1][1])

#correlation_value_with_snow_depth = Statistics.corr(taxi_data_values, weather_data_values)

tabSeparated =  joined_weather_taxi_trips.map(lambda x: str(x[0])+"\t"+str(x[1][0])+"\t"+str(x[1][1]))

#tabSeparated.saveAsTextFile("pickup_date_snow_depth.out")


####################################################################################################################################################
#Per season

taxi_data = taxi_data_1.map(lambda entry: (datetime.datetime.strptime(entry[1], '%Y-%m-%d %H:%M:%S').date(),checkPickUpDateValid(entry[1]))).filter(lambda x: x[1] == "Valid") #.map(lambda x: (x[0],1)).reduceByKey(lambda x,y: x+y)

winter_taxi_data = taxi_data.filter(lambda x: x[0].month in [1,2,3]).map(lambda x: (str(x[0]),1)).reduceByKey(lambda x,y: x+y)

#joined_weather_taxi_trips =winter_taxi_data.join(weather_data_temperature)
(joined_weather_taxi_trips,correlation_value_with_temperature_winter) = calculateCorrelation(winter_taxi_data,weather_data_temperature)

tabSeparated =  joined_weather_taxi_trips.map(lambda x: str(x[0])+"\t"+str(x[1][0])+"\t"+str(x[1][1]))
#tabSeparated.saveAsTextFile("pickup_date_winter_months_temperature_frequency.out")

#taxi_data_values = joined_weather_taxi_trips.map(lambda x:x[1][0])
#weather_data_values = joined_weather_taxi_trips.map(lambda x:x[1][1])

#correlation_value_with_temperature_winter= Statistics.corr(taxi_data_values, weather_data_values)
############ SUMMER ####################################
summer_taxi_data = taxi_data.filter(lambda x: x[0].month in [6,7,8]).map(lambda x: (str(x[0]),1)).reduceByKey(lambda x,y: x+y)

#joined_weather_taxi_trips =summer_taxi_data.join(weather_data_temperature)
(joined_weather_taxi_trips,correlation_value_with_temperature_summer) = calculateCorrelation(summer_taxi_data,weather_data_temperature)


tabSeparated =  joined_weather_taxi_trips.map(lambda x: str(x[0])+"\t"+str(x[1][0])+"\t"+str(x[1][1]))
#tabSeparated.saveAsTextFile("pickup_date_summer_months_temperature_frequency.out")

#taxi_data_values = joined_weather_taxi_trips.map(lambda x:x[1][0])
#weather_data_values = joined_weather_taxi_trips.map(lambda x:x[1][1])

#correlation_value_with_temperature_summer= Statistics.corr(taxi_data_values, weather_data_values)

################### FALL ###################################
fall_taxi_data = taxi_data.filter(lambda x: x[0].month in [9,10,11,12]).map(lambda x: (str(x[0]),1)).reduceByKey(lambda x,y: x+y)

#joined_weather_taxi_trips =fall_taxi_data.join(weather_data_temperature)
(joined_weather_taxi_trips,correlation_value_with_temperature_fall) = calculateCorrelation(fall_taxi_data,weather_data_temperature)


tabSeparated =  joined_weather_taxi_trips.map(lambda x: str(x[0])+"\t"+str(x[1][0])+"\t"+str(x[1][1]))
#tabSeparated.saveAsTextFile("pickup_date_fall_months_temperature_frequency.out")

#taxi_data_values = joined_weather_taxi_trips.map(lambda x:x[1][0])
#weather_data_values = joined_weather_taxi_trips.map(lambda x:x[1][1])

#correlation_value_with_temperature_fall = Statistics.corr(taxi_data_values, weather_data_values)

f = open( 'correlation_output_weather.out', 'w' )

f.write( 'Correlation value of number of trips with temperature = ' + str(correlation_value_with_temperature) + '\n' )
f.write( 'Correlation value of number of trips with windspeed = ' + str(correlation_value_with_windspeed) + '\n' )
f.write( 'Correlation value of number of trips with precipitation = ' + str(correlation_value_with_precipitation) + '\n' )
f.write( 'Correlation value of number of trips with snow depth = ' + str(correlation_value_with_snow_depth) + '\n' )
f.write( 'Correlation value of number of trips with temperature for winter months = ' + str(correlation_value_with_temperature_winter) + '\n' )
f.write( 'Correlation value of number of trips with temperature for summer months = ' + str(correlation_value_with_temperature_summer) + '\n' )
f.write( 'Correlation value of number of trips with temperature for fall months = ' + str(correlation_value_with_temperature_fall) + '\n' )
f.close()
	
sc.stop()
	

