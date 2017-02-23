#!/usr/bin/python
import os
import sys

for line in sys.stdin:
	entry = line.strip().split(",")
	inp_file = os.environ.get(mapreduce_map_input_file)
	if 'parking' in inp_file:
		print '%s\t%s, %s, %s, %s' % (entry[0],entry[14],entry[6],entry[2],entry[1])
	elif 'open' in inp_file:
		print '%s\t' % entry[0]
	else:
		raise 'CUSTOMMMMMMMError'
