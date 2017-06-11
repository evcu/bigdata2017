from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from myUtils import *
from validation_utils import *

import datetime



sc = SparkContext()
sc.addPyFile("myUtils.py")
sc.addPyFile("validation_utils.py")

fields = getFieldDic()
(taxi_data,prefix) = readFiles2({2016:range(1,7),2015:range(1,13),2014:range(1,13),2013:range(1,13)},sc)

field = taxi_data.map(lambda entry: (entry[1], entry[18])).map(lambda entry: (entry[0],entry[1],checkPickUpDateValid(entry[0]))).filter(lambda x: x[2] == "Valid")
valid_records = field.map(lambda x: (str(datetime.datetime.strptime(x[0], '%Y-%m-%d %H:%M:%S').date()),x[1],checkAmountValid(x[1]))).filter(lambda x: x[2] == "Valid").map(lambda x: (x[0],float(x[1])))
filtered_valid_records = valid_records.reduceByKey(lambda x,y: x+y)

tabSeparated =  filtered_valid_records.map(lambda x: x[0]+"\t"+str(x[1])) 
tabSeparated.saveAsTextFile("total_amount_per_date_frequency.out")
	
sc.stop()
	

