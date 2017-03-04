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

    def mapper(e):
        if e[16] != 'NY':
            return ('OTHER',1)
        else:
            return ('NY',1)

    vials = file1.map(mapper)

    res = vials.reduceByKey(add).map(lambda x: '%s\t%d'% x)
    res.saveAsTextFile("task4.out")
    sc.stop()
