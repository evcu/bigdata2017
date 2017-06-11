from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from myUtils import readFiles,getSomeFileNames,getAllFileNames

def checkValid(amount):
                if amount:
                        try:
                                num = float(amount)
                                if  num > 0:
                                        return "Valid"
                                elif num == 0:
                                        return "Valid_Zero_total_amount"
                                else:
                                        return "Invalid_Negative_total_amount"
                        except ValueError:
                                return "Invalid_NotFloat"
                else:
                        return "Invalid_Null"

valid_records = None
if __name__ == "__main__":
	sc = SparkContext()
	filenames = getAllFileNames()
        (taxi_data,prefix) = readFiles(filenames,sc)
		
	extra_amount = taxi_data.map(lambda entry: (checkValid(entry[13]),1)).reduceByKey(lambda x,y: x+y)
	
	tabSeparated =  extra_amount.map(lambda x: x[0]+"\t"+str(x[1])) 
    	tabSeparated.saveAsTextFile("extra_amount_valid_report.out")
	
	valid_records = taxi_data.map(lambda entry: (checkValid(entry[13]),entry)).filter(lambda x: x[0] == "Valid").map(lambda x: x[1])

	sc.stop()
	

