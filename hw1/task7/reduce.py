#!/usr/bin/python
import sys

c_key = float('-inf')
c_sum = [0,0]
for line in sys.stdin:
	key, value = line.strip().split('\t',1)
	vals = map(int,value.split())
	if key == c_key:
		c_sum[0] += vals[0]
		c_sum[1] += vals[1]
	else:
		if c_key != float('-inf'):
			print '%s\t%.2f, %.2f' %(c_key,c_sum[0]/8.0,c_sum[1]/23.0)
		c_key = key
		c_sum = [vals[0],vals[1]]
print '%s\t%.2f, %.2f' %(c_key,c_sum[0]/8.0,c_sum[1]/23.0)