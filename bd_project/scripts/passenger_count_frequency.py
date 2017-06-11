from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    	sc = SparkContext()
	csvfile = sc.textFile('data/yellow_tripdata_2016-01.csv,data/yellow_tripdata_2016-02.csv,data/yellow_tripdata_2016-03.csv,data/yellow_tripdata_2016-04.csv,data/yellow_tripdata_2016-05.csv,data/yellow_tripdata_2016-06.csv')
	
	header = csvfile.first()

	csvfile = csvfile.filter(lambda line : line != header)

    	taxi_data = csvfile.mapPartitions(lambda x: reader(x))
	
	passenger_count = taxi_data.map(lambda entry: (entry[3],1)).reduceByKey(lambda x,y: x+y)
	
	tabSeparated =  passenger_count.map(lambda x: x[0]+"\t"+str(x[1])) 
    	tabSeparated.saveAsTextFile(sys.argv[0].split('.')[0] + "_valid.out")
	
	sc.stop()
	

