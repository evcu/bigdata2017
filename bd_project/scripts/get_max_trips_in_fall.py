from __future__ import print_function

import sys
import numpy
import subprocess
import math
from pyspark.mllib.stat import Statistics

from operator import add
from pyspark import SparkContext
from csv import reader
import datetime
from myUtils import *
from validation_utils import *

sc = SparkContext()
sc.addPyFile("myUtils.py")
sc.addPyFile("validation_utils.py")


fields = getFieldDic()
(taxi_data,prefix) = readAllFiles(sc)

def getDateMonth(date_text, months):
    if date_text is not None:
        try:
            given_date = datetime.datetime.strptime(date_text, '%Y-%m-%d %H:%M:%S')
            year = given_date.year
            month = given_date.month
            date_string = str(year)+"\t"+str(month)
            if month in months and year == 2015:
                return date_string
            else:
                return "NOT_VALID"
        except ValueError:
            return "NOT_VALID"
    else:
        return "NOT_VALID"

 
taxi_data = cleanByFields(taxi_data, ['tpep_pickup_datetime','pickup_longitude','pickup_latitude'])
taxi_data_valid_records_summer = taxi_data.map(lambda entry: ((getDateMonth(entry[fields['tpep_pickup_datetime']],[7,8]),entry[fields['pickup_longitude']],entry[fields['pickup_latitude']]),1)).filter(lambda x: x[0][0] != "NOT_VALID").reduceByKey(lambda x,y : x+y)

taxi_data_valid_records_fall = taxi_data.map(lambda entry: ((getDateMonth(entry[fields['tpep_pickup_datetime']],[9,10]),entry[fields['pickup_longitude']],entry[fields['pickup_latitude']]),1)).filter(lambda x: x[0][0] != "NOT_VALID").reduceByKey(lambda x,y : x+y)


taxi_data_valid_records_summer.sortBy(lambda x: x[1],False).map(lambda x: str(x[0][0])+"\t"+str(x[0][1])+"\t"+str(x[0][2])+"\t"+str(x[1])).saveAsTextFile("fall_gps_coordinates_frequency_summer.out") 

taxi_data_valid_records_fall.sortBy(lambda x: x[1],False).map(lambda x: str(x[0][0])+"\t"+str(x[0][1])+"\t"+str(x[0][2])+"\t"+str(x[1])).saveAsTextFile("fall_gps_coordinates_frequency_fall.out")

filtered_summer_trips = taxi_data_valid_records_summer.filter(lambda x: "8" in x[0][0]).map(lambda x: ((str(x[0][1]),str(x[0][2])),x[1])).sortBy(lambda x: x[1],False)
filtered_fall_trips =  taxi_data_valid_records_fall.filter(lambda x: "10" in x[0][0]).map(lambda x: ((str(x[0][1]),str(x[0][2])),x[1])).sortBy(lambda x: x[1],False)

joined_records = filtered_summer_trips.fullOuterJoin(filtered_fall_trips)

joined_records.map(lambda x: x[0][0]+"\t"+x[0][1]+"\t"+str(x[1][0])+"\t"+str(x[1][1])).saveAsTextFile("fall_gps_coordinates_frequency_aug_oct.out")
