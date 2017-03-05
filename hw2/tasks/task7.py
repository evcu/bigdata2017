from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

weekend_days = set([5,6,12,13,19,20,26,27])

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    file1 = sc.textFile(sys.argv[1], 1).mapPartitions(lambda x: reader(x))

    def mapper1(e):
        if int(e[1].split('-')[-1]) in  weekend_days:
            return (entry[2] , 1.0)

    def mapper2(e):
        if int(e[1].split('-')[-1]) not in  weekend_days:
            return (entry[2] , 1.0)

    we = file1.map(mapper1).groupByKey().map(lambda (x,v): (x,len(v)/8.0))
    wd = file1.map(mapper2).groupByKey().map(lambda (x,v): (x,len(v)/23.0))
    res = we.union(wd).map(lambda k,v:'%s\t%.2f, %.2f' % (v[0],v[1]))
    res.saveAsTextFile("task7.out")
    sc.stop()
