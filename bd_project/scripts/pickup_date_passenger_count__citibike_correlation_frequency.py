from __future__ import print_function

import sys
import numpy
import subprocess
from pyspark.mllib.stat import Statistics

from operator import add
from pyspark import SparkContext
from csv import reader
import datetime
from myUtils import *
from validation_utils import *


def getDate(date_text):
    try:
        given_date = datetime.datetime.strptime(date_text, '%Y-%m-%d %H:%M:%S')
        return given_date.date() 
    except ValueError:
        try:
            given_date = datetime.datetime.strptime(date_text, '%m/%d/%Y %H:%M:%S')
            return given_date.date()
        except ValueError:
            try:
                given_date = datetime.datetime.strptime(date_text, '%m/%d/%Y %H:%M')
                return given_date.date()
            except ValueError:
                return None


def readCitibikeData(sc):
    file_path = '/user/dv697/data/citibike/*'
    csvfile = sc.textFile(file_path)
    csvfile = csvfile.map(lambda line: line.replace('"','' ))
    header = csvfile.first()
    csvfile = csvfile.filter(lambda line : line != header)
    citibike_data = csvfile.mapPartitions(lambda x: reader(x)).map(lambda x: (str(getDate(x[1].strip().replace('"','' ))),1))
    return citibike_data


def calculateCorrelation(rdd1, rdd2):
    joined_rdd = rdd1.join(rdd2).sortByKey()

    rdd1_values = joined_rdd.map(lambda x:x[1][0])
    rdd2_values = joined_rdd.map(lambda x:x[1][1])
    correlation_value = Statistics.corr(rdd1_values, rdd2_values)
    return (joined_rdd,correlation_value)


sc = SparkContext()
sc.addPyFile("myUtils.py")
sc.addPyFile("validation_utils.py")


fields = getFieldDic()
(taxi_data,prefix) = readAllFiles(sc)


field = taxi_data.map(lambda entry: (entry[1],entry[3],checkPickUpDateValid(entry[1])))
filtered_taxi_valid_records = field.filter(lambda x: x[2] == "Valid")

date_taxi_records = filtered_taxi_valid_records.map(lambda x: (str(getDate(x[0])),1)).reduceByKey(lambda x,y: x+y)

citibike_data = readCitibikeData(sc)
citibike_data = citibike_data.reduceByKey(lambda x,y: x+y)

(joined_citibike_taxi_trips,correlation_value) = calculateCorrelation(date_taxi_records,citibike_data)

###############################################################################################################
passenger_count_valid_records = filtered_taxi_valid_records.filter(lambda x: int(x[1]) == 1)

date_taxi_passenger_count_1_records = passenger_count_valid_records.map(lambda x: (str(getDate(x[0])),1)).reduceByKey(lambda x,y: x+y)
(joined_citibike_taxi_trips_passenger_count,passenger_count_correlation_value) = calculateCorrelation(date_taxi_passenger_count_1_records,citibike_data)

############################ Summer + Spring Months #####################################################################
summer_spring_taxi_data = filtered_taxi_valid_records.filter(lambda x: getDate(x[0]).month in [4,5,6,7,8]).map(lambda x: (str(getDate(x[0])),1)).reduceByKey(lambda x,y: x+y)
(joined_citibike_taxi_trips_passenger_count_summer_spring,spring_summer_correlation_value) = calculateCorrelation(summer_spring_taxi_data,citibike_data)

######################################################################################################
passenger_count_valid_records_summer_spring = filtered_taxi_valid_records.filter(lambda x: int(x[1]) == 1 and  getDate(x[0]).month in [4,5,6,7,8])

date_taxi_passenger_count_1_records_summer_spring = passenger_count_valid_records_summer_spring.map(lambda x: (str(getDate(x[0])),1)).reduceByKey(lambda x,y: x+y)

(joined_citibike_taxi_trips_passenger_count_summer_spring,passenger_count_correlation_value_summer_spring) = calculateCorrelation(date_taxi_passenger_count_1_records_summer_spring,citibike_data)

############################### 2015 and 2016 ###############################################################################################
summer_spring_taxi_data_2015 = filtered_taxi_valid_records.filter(lambda x: getDate(x[0]).month in [4,5,6,7,8] and getDate(x[0]).year ==2015).map(lambda x: (str(getDate(x[0])),1)).reduceByKey(lambda x,y: x+y)
(joined_citibike_taxi_trips_passenger_count_summer_spring_2015,spring_summer_correlation_value_2015) = calculateCorrelation(summer_spring_taxi_data_2015,citibike_data)

summer_spring_taxi_data_2016 = filtered_taxi_valid_records.filter(lambda x: getDate(x[0]).month in [4,5,6,7,8] and getDate(x[0]).year ==2016).map(lambda x: (str(getDate(x[0])),1)).reduceByKey(lambda x,y: x+y)
(joined_citibike_taxi_trips_passenger_count_summer_spring_2016,spring_summer_correlation_value_2016) = calculateCorrelation(summer_spring_taxi_data_2016,citibike_data)


######################################################################################################
passenger_count_valid_records_summer_spring_16 = filtered_taxi_valid_records.filter(lambda x: int(x[1]) == 1 and  getDate(x[0]).month in [4,5,6,7,8] and getDate(x[0]).year ==2016)

date_taxi_passenger_count_1_records_summer_spring_16 = passenger_count_valid_records_summer_spring.map(lambda x: (str(getDate(x[0])),1)).reduceByKey(lambda x,y: x+y)

(joined_citibike_taxi_trips_passenger_count_summer_spring_16,passenger_count_correlation_value_summer_spring_16) = calculateCorrelation(date_taxi_passenger_count_1_records_summer_spring_16,citibike_data)

passenger_count_valid_records_summer_spring_15 = filtered_taxi_valid_records.filter(lambda x: int(x[1]) == 1 and  getDate(x[0]).month in [4,5,6,7,8] and getDate(x[0]).year ==2015)

date_taxi_passenger_count_1_records_summer_spring_15 = passenger_count_valid_records_summer_spring.map(lambda x: (str(getDate(x[0])),1)).reduceByKey(lambda x,y: x+y)

(joined_citibike_taxi_trips_passenger_count_summer_spring_15,passenger_count_correlation_value_summer_spring_15) = calculateCorrelation(date_taxi_passenger_count_1_records_summer_spring_15,citibike_data)


f = open( 'correlation_output_citibike_2.out', 'w' )

f.write( 'Correlation value of number of trips with citibike number of trips = ' + str(correlation_value) + '\n' )
f.write( 'Correlation value of number of trips with citibike number of trips with passenger count 1 = ' + str(passenger_count_correlation_value) + '\n' )
f.write( 'Correlation value of number of trips with citibike number of trips in summer and Spring months = ' + str(spring_summer_correlation_value) + '\n' )
f.write( 'Correlation value of number of trips with citibike number of trips in summer and Spring months with passenger count 1= ' + str(passenger_count_correlation_value_summer_spring) + '\n' )

f.write( 'Correlation value of number of trips with citibike number of trips in summer and Spring months in 2015 = ' + str(spring_summer_correlation_value_2015) + '\n' )
f.write( 'Correlation value of number of trips with citibike number of trips in summer and Spring months with passenger count 1 in 2015= ' + str(passenger_count_correlation_value_summer_spring_15) + '\n' )

f.write( 'Correlation value of number of trips with citibike number of trips in summer and Spring months in 2016 = ' + str(spring_summer_correlation_value_2016) + '\n' )
f.write( 'Correlation value of number of trips with citibike number of trips in summer and Spring months with passenger count 1 in 2016= ' + str(passenger_count_correlation_value_summer_spring_16) + '\n' )


f.close()
tabSeparated =  joined_citibike_taxi_trips.map(lambda x: str(x[0])+"\t"+str(x[1][0])+"\t"+str(x[1][1]))
	
tabSeparated.saveAsTextFile("taxi_data_vs_citbike_frequency.out")

tabSeparated =  joined_citibike_taxi_trips_passenger_count.map(lambda x: str(x[0])+"\t"+str(x[1][0])+"\t"+str(x[1][1]))

tabSeparated.saveAsTextFile("taxi_data_vs_citbike_frequency_passenger_count_1.out")
 	
sc.stop()
	

