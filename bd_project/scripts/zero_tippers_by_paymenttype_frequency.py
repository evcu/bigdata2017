from __future__ import print_function

import sys
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

(taxi_data,prefix) = readFiles2({2016:range(1,7),2015:range(1,13),2014:range(1,13),2013:range(1,13)},sc)
	
field = taxi_data.map(lambda entry: ((entry[fields['tip_amount']],checkAmountValid(entry[fields['tip_amount']])),(checkPaymentTypeValid(entry[fields['payment_type']]), entry[fields['payment_type']])))
filtered_valid_records = field.filter(lambda x: x[0][1] == "Valid_Zero_amount" and x[1][0] == "Valid" ).map(lambda x: (x[1][1],1)).reduceByKey(lambda x,y: x+y)
	
tabSeparated =  filtered_valid_records.map(lambda x: x[0]+"\t"+str(x[1])) 
tabSeparated.saveAsTextFile("zero_tippers_payment_type_frequency.out")
	
sc.stop()
	

