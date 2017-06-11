from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
import datetime
from myUtils import *
from validation_utils import *


def checkValid(date_text):
    if date_text is not None:
        try:
            given_date = datetime.datetime.strptime(date_text, '%Y-%m-%d %H:%M:%S')
            year = given_date.year
            month = given_date.month
            hour = given_date.hour
            day_of_the_week = given_date.isoweekday()
            if year >= 2013 and year <= 2016:
                return (given_date.date(), hour ,"Valid")
            else:
                return (given_date, hour,"Invalid_year")
        except ValueError:
                return (date_text,"","Invalid_date")
    else:
        return (date_text,"","Invalid_Null_date")



sc = SparkContext()
sc.addPyFile("myUtils.py")
sc.addPyFile("validation_utils.py")

#(taxi_data,prefix) = readFiles2({2016:range(1,7),2015:range(1,13),2014:range(1,13),2013:range(1,13)},sc)
(taxi_data,prefix) = readAllFiles(sc)
	
field = taxi_data.map(lambda entry: checkValid(entry[1]))
filtered_valid_records = field.filter(lambda x: x[2] == "Valid").map(lambda x: (str(x[0])+"\t"+str(x[1]),1)).reduceByKey(lambda x,y: x+y)
	
filtered_valid_records.map(lambda x: x[0]+"\t"+str(x[1])).saveAsTextFile("pickup_time_frequency.out")
filtered_valid_records.sortBy(lambda x: x[1],False).map(lambda x: x[0]+"\t"+str(x[1])).saveAsTextFile("pickup_time_frequency_max.out")
filtered_valid_records.sortBy(lambda x: x[1],True).map(lambda x: x[0]+"\t"+str(x[1])).saveAsTextFile("pickup_time_frequency_min.out")

	
sc.stop()
	

