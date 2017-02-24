#!/usr/bin/python
import os
import sys
import csv 

d = {}
weekend_days = {5,6,12,13,19,20,26,27}

for entry in  csv.reader(sys.stdin, quotechar='"', delimiter=',',
               quoting=csv.QUOTE_ALL, skipinitialspace=True):
	if entry[2] not in d:
		d[entry[2]] = [0,0]
	if int(entry[1].split('-')[-1]) in  weekend_days:
		d[entry[2]][0] += 1
	else:
		d[entry[2]][1] += 1

for k in d.keys():
    print '%s\t%d %d' % (k,d[k][0],d[k][1])
		
