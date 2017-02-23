#!/usr/bin/python
import os
import sys

for entry in  csv.reader(sys.stdin, quotechar='"', delimiter=',',
               quoting=csv.QUOTE_ALL, skipinitialspace=True):
	print '%s\t%d' % (entry[2],1)
