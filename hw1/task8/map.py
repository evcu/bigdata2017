#!/usr/bin/python
import os
import sys
import csv 


for entry in  csv.reader(sys.stdin, quotechar='"', delimiter=',',
               quoting=csv.QUOTE_ALL, skipinitialspace=True):
	for w in entry
		print '%s\t1' % w
		
