from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
import datetime
from myUtils import *
from validation_utils import *


def getDateHour(date_text):
    given_date = datetime.datetime.strptime(date_text, '%Y-%m-%d %H:%M:%S')
    year = given_date.year
    month = given_date.month
    hour = given_date.hour
    day_of_the_week = given_date.isoweekday()
    return (given_date.date(), hour ,"Valid")



sc = SparkContext()
sc.addPyFile("myUtils.py")
sc.addPyFile("validation_utils.py")

(taxi_data,prefix) = readFiles2({2016:range(1,7),2015:range(1,13),2014:range(1,13),2013:range(1,13)},sc)
	
field = taxi_data.map(lambda entry: (entry[1],checkPickUpDateValid(entry[1])))
filtered_valid_records = field.filter(lambda x: x[1] == "Valid").map(lambda x: (getDateHour(x[0]))).map(lambda x: (str(x[0])+"\t"+str(x[1]),1)).reduceByKey(lambda x,y: x+y)
	
tabSeparated =  filtered_valid_records.map(lambda x: x[0]+"\t"+str(x[1])) 
tabSeparated.saveAsTextFile("pickup_date_and_time_frequency.out")
	
sc.stop()
	

