#!/usr/bin/python
import sys

flag = False
c_key = -1
c_val = ''
for line in sys.stdin:
	key, value = line.strip().split('\t',1)
	if key != c_key:
		if flag and cval:
			print cval
		c_val = ''
		flag = False
		c_key = key
	if value == 'o':
		flag=True
	else:
		c_val = value