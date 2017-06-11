from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from myUtils import readFiles,getSomeFileNames,getAllFileNames


def checkPassengerCountValid(passenger_count):
                try:
                        int(passenger_count)
                        if int(passenger_count) >= 0 and int(passenger_count) < 10:
                                return "Valid"
                        else:
                                return "Invalid_not_within_range_"+passenger_count
                except ValueError:
                        return "Invalid_not_integer_"+passenger_count


if __name__ == "__main__":
	sc = SparkContext()
	#(taxi_data,prefix) = readFiles(['data/yellow_tripdata_2016-01.csv','data/yellow_tripdata_2016-02.csv','data/yellow_tripdata_2016-03.csv','data/yellow_tripdata_2016-04.csv','data/yellow_tripdata_2016-05.csv','data/yellow_tripdata_2016-06.csv'],sc )
	filenames = getAllFileNames()
        (taxi_data,prefix) = readFiles(filenames,sc)

		
	vendorID = taxi_data.map(lambda entry: (checkValid(entry[3]),1)).reduceByKey(lambda x,y: x+y)
	
	tabSeparated =  vendorID.map(lambda x: x[0]+"\t"+str(x[1])) 
    	tabSeparated.saveAsTextFile("passenger_count_valid.out")
	
	sc.stop()
	

