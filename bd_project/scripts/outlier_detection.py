from __future__ import print_function

import sys
import math
from operator import add
from pyspark import SparkContext
import datetime
from myUtils import *
from validation_utils import *

def checkOutlier(value, mean, sd):
    score = (value - mean)/sd
    if math.fabs(score) > 4:
        return "Outlier"
    else:
        return "Valid"

sc = SparkContext()
sc.addPyFile("myUtils.py")
sc.addPyFile("validation_utils.py")

fields = getFieldDic()
(taxi_data,prefix) = readFiles2({2016:range(1,7),2015:range(1,13),2014:range(1,13),2013:range(1,13)},sc) 

trip_distance = taxi_data.map(lambda entry: (entry[4],checkTripDistanceValid(entry[4])))
valid_records =  trip_distance.filter(lambda x: x[1] == "Valid")
mean = valid_records.map(lambda x: float(x[0])).mean()
sd = valid_records.map(lambda x: float(x[0])).stdev()    
valid_records = valid_records.map(lambda x: (checkOutlier(float(x[0]), mean, sd),1))
not_valid_records = trip_distance.filter(lambda x: x[1] != "Valid").map(lambda x: (x[1],1)).reduceByKey(lambda x,y: x+y).map(lambda x: x[0]+"\t"+str(x[1]))
valid_records_print = valid_records.reduceByKey(lambda x,y: x+y).map(lambda x: x[0]+"\t"+str(x[1]))
valid_records_print.union(not_valid_records).saveAsTextFile("trip_distance_valid_including_outlier.out")



total_amount = taxi_data.map(lambda entry: (entry[18],checkAmountValid(entry[18])))
valid_records =  total_amount.filter(lambda x: x[1] == "Valid")
mean = valid_records.map(lambda x: float(x[0])).mean()
sd = valid_records.map(lambda x: float(x[0])).stdev()
valid_records = valid_records.map(lambda x: (checkOutlier(float(x[0]), mean, sd),1))
not_valid_records = total_amount.filter(lambda x: x[1] != "Valid").map(lambda x: (x[1],1)).reduceByKey(lambda x,y: x+y).map(lambda x: x[0]+"\t"+str(x[1]))
valid_records_print = valid_records.reduceByKey(lambda x,y: x+y).map(lambda x: x[0]+"\t"+str(x[1]))
valid_records_print.union(not_valid_records).saveAsTextFile("total_amount_valid_including_outlier.out")



