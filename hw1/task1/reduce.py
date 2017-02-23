#!/usr/bin/python
import sys

flag = True
c_key = -1
c_val = ''
for line in sys.stdin:
	key, value = line.strip().split('\t',1)
	if key != c_key:
		if flag and c_val:
			print c_val
		c_val = ''
		flag = True
		c_key = key
	if value == 'o':
		flag = False
	else:
		c_val = value
if flag and c_val:
	print c_val