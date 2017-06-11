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

 
taxi_data = cleanByFields(taxi_data, ['trip_distance','pickup_longitude','pickup_latitude','dropoff_longitude','dropoff_latitude'])
filtered_records = taxi_data.map(lambda entry: (entry[fields['trip_distance']],entry[fields['pickup_longitude']],entry[fields['pickup_latitude']],entry[fields['dropoff_longitude']],entry[fields['dropoff_latitude']])).filter(lambda x: x[0] == 0 and (x[1] != x[3] or x[2] != x[4]))

f = open( 'count_of_trips_with_distance_0_diff_corrdinates.out', 'w' )
f.write( 'Count of number of trips if with distance 0 and different gps coordinates = ' + str(filtered_records.count()) + '\n' )

