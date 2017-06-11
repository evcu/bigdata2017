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



def removeTripDistanceOutlier(trip_distance):
    mean = trip_distance.map(lambda x: float(x)).mean()
    sd = trip_distance.map(lambda x: float(x)).stdev()
    valid_records = trip_distance.map(lambda x: (x,checkOutlier(float(x), mean, sd))).filter(lambda x: x[1]=="Valid")
    return valid_records.map(lambda x: x[0])    


taxi_data = cleanByFields(taxi_data, ['trip_distance'])
taxi_data_required_field = removeTripDistanceOutlier(taxi_data.map(lambda entry: (float(entry[fields['trip_distance']]))))#.filter(lambda x: x[0] > 0 and x[1] > 0)#.map(lambda x: ((x[0],x[1]),1)).reduceByKey(lambda x,y : x+y)



histogram_rdd = taxi_data_required_field.histogram([0,2.5,5,10,20,30,40,50,100,250,500,1000])

#tabSeparated =  d_rdd.map(lambda x: str(x[0][0])+"\t"+str(x[0][1])+"\t"+str(x[1])) 
f = open( "trip_distance_histogram_1.out", 'w' )
f.write( str(histogram_rdd) )
f.close()


