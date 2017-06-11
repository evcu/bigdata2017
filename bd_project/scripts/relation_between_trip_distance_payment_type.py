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

def checkOutlier(value, mean, sd):
    score = (value - mean)/sd
    if math.fabs(score) > 4:
        return "Outlier"
    else:
        return "Valid"



def removeTripDistanceOutlier(taxi_data):
    mean = taxi_data.map(lambda x: float(x[0])).mean()
    sd = taxi_data.map(lambda x: float(x[0])).stdev()
    valid_records = taxi_data.map(lambda x: (x,checkOutlier(float(x[0]), mean, sd))).filter(lambda x: x[1]=="Valid")
    return valid_records.map(lambda x: x)    

def getBinNUmber(trip_distance):
    if 0 <= trip_distance < 2.5:
        return 0
    elif 2.5 <= trip_distance < 5:
        return 2.5
    elif 5 <= trip_distance < 10:
        return 5
    elif 10 <= trip_distance < 20:
        return 10
    elif 20 <= trip_distance < 30:
        return 20
    elif 30 <= trip_distance < 40:
        return 30
    elif 40 <= trip_distance < 50:
        return 40
    elif 50 <= trip_distance < 100:
        return 50
    elif 100 <= trip_distance < 250:
        return 100
    elif 250 <= trip_distance < 500:
        return 250
    elif 500 <= trip_distance < 1000:
        return 500
    elif 1000 <= trip_distance :
        return 1000
 
taxi_data = cleanByFields(taxi_data, ['trip_distance','payment_type']).filter(lambda entry: float(entry[fields['trip_distance']]) > 0  and float(entry[fields['trip_distance']]) < 1000)
taxi_data_valid_records = removeTripDistanceOutlier(taxi_data.map(lambda entry: (float(entry[fields['trip_distance']]),int(entry[fields['payment_type']]))))#.filter(lambda x: x[0] > 0 and x[1] > 0)#.map(lambda x: ((x[0],x[1]),1)).reduceByKey(lambda x,y : x+y)

taxi_data_valid_records = taxi_data_valid_records.map(lambda entry: ((str(getBinNUmber(float(entry[0][0]))),entry[0][1]),1))
#taxi_data_valid_records = taxi_data_valid_records.map(lambda entry: ((entry[0][0],entry[0][1]),1))
taxi_data_valid_records = taxi_data_valid_records.reduceByKey(lambda x,y : x+y).sortByKey()
taxi_data_valid_records_payment_type_1_2 =taxi_data_valid_records.filter(lambda x: x[0][1]==1 or x[0][1]==2)


tabSeparated =  taxi_data_valid_records_payment_type_1_2.map(lambda x: str(x[0][0])+"\t"+str(x[0][1])+"\t"+str(x[1])) 
tabSeparated.saveAsTextFile("payment_type_1_2_trip_distance_relation.out")

tabSeparated =  taxi_data_valid_records.map(lambda x: str(x[0][0])+"\t"+str(x[0][1])+"\t"+str(x[1]))
tabSeparated.saveAsTextFile("payment_type_trip_distance_relation.out")



