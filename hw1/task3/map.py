#!/usr/bin/python
import os
import sys
import csv 
from collections import defaultdict

d = {}

for entry in  csv.reader(sys.stdin, quotechar='"', delimiter=',',
               quoting=csv.QUOTE_ALL, skipinitialspace=True):
	if entry[2] in d:
		d[entry[2]][0] += float(entry[12])
		d[entry[2]][1] += 1
	else:
		d[entry[2]] = [float(entry[12]),1]

for k in d.keys():
	print '%s\t%f %d' % (k,d[k][0],d[k][1])
