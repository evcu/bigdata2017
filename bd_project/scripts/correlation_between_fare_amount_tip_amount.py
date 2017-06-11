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

sc = SparkContext()
sc.addPyFile("myUtils.py")
sc.addPyFile("validation_utils.py")


fields = getFieldDic()
(taxi_data,prefix) = readAllFiles(sc)

taxi_data = cleanByFields(taxi_data, ['tip_amount','fare_amount'])
taxi_data_required_field = taxi_data.map(lambda entry: (float(entry[fields['tip_amount']]),float(entry[fields['fare_amount']]))).filter(lambda x: x[0] > 0 and x[1] > 0 and x[0] <= x[1])#.map(lambda x: ((x[0],x[1]),1)).reduceByKey(lambda x,y : x+y)

joined_rdd = taxi_data_required_field.sortByKey()

#rdd1_values = joined_rdd.map(lambda x:x[0][0])
#rdd2_values = joined_rdd.map(lambda x:x[0][1])
rdd1_values = joined_rdd.map(lambda x:x[0])
rdd2_values = joined_rdd.map(lambda x:x[1])
correlation_value = Statistics.corr(rdd1_values, rdd2_values)

f = open( 'correlation_output_tip_amount_vs_fare_amount.out', 'w' )
f.write( 'Correlation value of fare amount and tip amount = ' + str(correlation_value) + '\n' )

tabSeparated =  joined_rdd.map(lambda x: ((x[0],x[1]),1)).reduceByKey(lambda x,y : x+y).map(lambda x: str(x[0][0])+"\t"+str(x[0][1])+"\t"+str(x[1])) 
tabSeparated.saveAsTextFile("tip_amount_fare_amount.out")
