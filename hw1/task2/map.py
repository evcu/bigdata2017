#!/usr/bin/python
import os
import sys

for l in sys.stdin:
	entry = line.strip().split(",")
	print '%s\t%d' % (entry[2],1)
