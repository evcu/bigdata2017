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
    file2 = sc.textFile(sys.argv[2], 1).mapPartitions(lambda x: reader(x))

    vials = file1.map(lambda entry: (entry[0],'%s, %s, %s, %s' % (entry[14],entry[6],entry[2],entry[1])))
    opens = file2.map(lambda entry: (entry[0],'NotPaid'))
    vials.subtractByKey(opens).saveAsTextFile("task1.out")

    sc.stop()
