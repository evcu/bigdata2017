from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from myUtils import readFiles,getSomeFileNames,getAllFileNames
import datetime



def checkValid(date_text):
    if date_text is not None:
        try:
            given_date = datetime.datetime.strptime(date_text, '%Y-%m-%d %H:%M:%S')
            year = given_date.year
            if year >= 2013 and year <= 2016:
                return (str(given_date.date()),"Valid")
            else:
                return (str(given_date.date()),"Invalid_year")
        except ValueError:
            return (date_text,"Invalid_date")
    else:
        return (date_text,"Invalid_Null_date")



if __name__ == "__main__":
	sc = SparkContext()
	filenames = getAllFileNames()
	(taxi_data,prefix) = readFiles(filenames,sc)
		
	field = taxi_data.map(lambda entry: checkValid(entry[1]))
	filtered_valid_records = field.filter(lambda x: x[1] == "Valid").map(lambda x: (x[0],1)).reduceByKey(lambda x,y: x+y)
	
	tabSeparated =  filtered_valid_records.map(lambda x: x[0]+"\t"+str(x[1])) 
    	tabSeparated.saveAsTextFile("pickup_date_frequency.out")
	
	sc.stop()
	

