from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    file1 = sc.textFile(sys.argv[1], 1).mapPartitions(lambda x: reader(x))

    opens = file1.map(lambda entry: (entry[2],float(entry[12])))
    res = opens.groupByKey().map(lambda (x,v): '%s\t%.2f, %.2f' % (x,sum(v),sum(v)/len(v)))
    res.saveAsTextFile("task3.out")
    sc.stop()
