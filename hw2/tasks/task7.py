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

    def mapper(v):
        we = sum(v[1])
        wd = len(v[1])-we
        return '%s\t%.2f, %.2f' % (v[0],we/8.0,wd/23.0)

    res.saveAsTextFile("task7.out")
        sc.stop()


