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
				date_string = str(given_date.date())
                                if year >= 2013 and year <= 2016:
                                        return (date_string,"Valid")
                                else:
                                        return (date_string,"Invalid_year")
                        except ValueError:
                                return (date_text,"Invalid_date")
                else:
                        return (date_text,"Invalid_Null_date")



sc = SparkContext()
sc.addPyFile("myUtils.py")
sc.addPyFile("validation_utils.py")
fields = getFieldDic()

(taxi_data,prefix) = readFiles2({2016:range(1,7),2015:range(1,13),2014:range(1,13),2013:range(1,13)},sc)
	
field = taxi_data.map(lambda entry: (checkValid(entry[1]),(checkPaymentTypeValid(entry[fields['payment_type']]), entry[fields['payment_type']])))
filtered_valid_records = field.filter(lambda x: x[0][1] == "Valid" and x[1][0] == "Valid" and x[1][1] == "2").map(lambda x: (x[0][0],1)).reduceByKey(lambda x,y: x+y)
	
tabSeparated =  filtered_valid_records.map(lambda x: x[0]+"\t"+str(x[1])) 
tabSeparated.saveAsTextFile("cash_frequency.out")
	
sc.stop()
	

