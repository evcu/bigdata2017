#!/usr/bin/python
import os
import sys
import csv 

for entry in  csv.reader(sys.stdin, quotechar='"', delimiter=',',
               quoting=csv.QUOTE_ALL, skipinitialspace=True):
	print '%s, %s\t%d' % (entry[14],entry[16],1)
