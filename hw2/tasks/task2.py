from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    file1 = sc.textFile(sys.argv[1], 1).mapPartitions(lambda x: reader(x))

    vials = file1.map(lambda entry: (entry[2],1)).reduceByKey(lambda a,b: a+b)
    vials.map(lambda e: '%s\t%d' %(e[0],e[1])).saveAsTextFile("task2.out")
    sc.stop()
