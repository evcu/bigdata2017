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
        return ('%s, %s' % (e[14],e[16]) , 1)

    vials = file1.map(mapper)

    res = vials.reduceByKey(add).sortBy(lambda x: x[1],False).map(lambda x: '%s\t%d'% x).first()
    sc.parallelize(res).saveAsTextFile("task5.out")
    sc.stop()
