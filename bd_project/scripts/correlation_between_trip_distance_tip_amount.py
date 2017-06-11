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

def checkOutlier(value, mean, sd):
    score = (value - mean)/sd
    if math.fabs(score) > 4:
        return "Outlier"
    else:
        return "Valid"



def removeOutlier(rdd):
    tip_mean = rdd.map(lambda x: float(x[0])).mean()
    tip_sd = rdd.map(lambda x: float(x[0])).stdev()
    trip_distance_mean = rdd.map(lambda x: float(x[1])).mean()
    trip_distance_sd = rdd.map(lambda x: float(x[1])).stdev()

    valid_records = rdd.map(lambda x: (x[0],checkOutlier(float(x[0]), tip_mean, tip_sd), x[1], checkOutlier(float(x[1]),trip_distance_mean, trip_distance_sd))).filter(lambda x: x[1]=="Valid" and x[3] == "Valid")
    return valid_records.map(lambda x: (x[0],x[2]))

fields = getFieldDic()
(taxi_data,prefix) = readAllFiles(sc)

taxi_data = cleanByFields(taxi_data, ['tip_amount','trip_distance'])
taxi_data_required_field = taxi_data.filter(lambda entry: (float(entry[fields['tip_amount']]) < float(entry[fields['fare_amount']]) and float(entry[fields['trip_distance']]) > 0 and float(entry[fields['tip_amount']]) > 0 and  float(entry[fields['fare_amount']]) > 0 and float(entry[fields['trip_distance']]) < 1000 ))

taxi_data_valid_records = removeOutlier(taxi_data_required_field.map(lambda entry: (float(entry[fields['tip_amount']]),float(entry[fields['trip_distance']]))))#.map(lambda x: ((x[0],x[1]),1)).reduceByKey(lambda x,y : x+y)

joined_rdd = taxi_data_valid_records.sortByKey()

#rdd1_values = joined_rdd.map(lambda x:x[0][0])
#rdd2_values = joined_rdd.map(lambda x:x[0][1])
rdd1_values = joined_rdd.map(lambda x:x[0])
rdd2_values = joined_rdd.map(lambda x:x[1])
correlation_value = Statistics.corr(rdd1_values, rdd2_values)

f = open( 'correlation_output_tip_amount_vs_trip_distance.out', 'w' )
f.write( 'Correlation value of fare amount and trip distance = ' + str(correlation_value) + '\n' )

#tabSeparated =  joined_rdd.map(lambda x: str(x[0][0])+"\t"+str(x[0][1])+"\t"+str(x[1])) 
#tabSeparated.saveAsTextFile("tip_amount_trip_distance.out")
