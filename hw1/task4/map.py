#!/usr/bin/python
import os
import sys
import csv 
from collections import defaultdict

d = {'NY':0,'OTHER':0}

for entry in  csv.reader(sys.stdin, quotechar='"', delimiter=',',
               quoting=csv.QUOTE_ALL, skipinitialspace=True):
    if entry[16] == 'NY':
        d['NY'] += 1
    else:
        d['OTHER'] += 1

for k in d.keys():
    print '%s\t%d' % (k,d[k])
