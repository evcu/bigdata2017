from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from myUtils import readFiles,getSomeFileNames,getAllFileNames
import datetime

if __name__ == "__main__":
	sc = SparkContext()
	#(taxi_data,prefix) = readFiles(['data/yellow_tripdata_2016-01.csv','data/yellow_tripdata_2016-02.csv','data/yellow_tripdata_2016-03.csv','data/yellow_tripdata_2016-04.csv','data/yellow_tripdata_2016-05.csv','data/yellow_tripdata_2016-06.csv'],sc )
	filenames = getAllFileNames()
        (taxi_data,prefix) = readFiles(filenames,sc)

	def checkValid(date_text):
		if date_text is not None:
			try:
				given_date = datetime.datetime.strptime(date_text, '%Y-%m-%d %H:%M:%S')
				year = given_date.year
				if year >= 2013 and year <= 2016:
					return "Valid"
				else:
					return "Invalid_year_"+date_text
			except ValueError:
				return "Invalid_date_"+date_text
		else:
			return "Invalid_Null_date"
		
	vendorID = taxi_data.map(lambda entry: (checkValid(entry[1]),1)).reduceByKey(lambda x,y: x+y)
	
	tabSeparated =  vendorID.map(lambda x: x[0]+"\t"+str(x[1])) 
    	tabSeparated.saveAsTextFile("pickup_date_valid_report.out")
	
	sc.stop()
	

