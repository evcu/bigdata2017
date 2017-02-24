#!/usr/bin/python
import sys

c_key = float('-inf')
c_sum = 0
c_max = 0
c_res = None
for line in sys.stdin:
	key, value = line.strip().split('\t',1)
	if key == c_key:
		c_sum += 1
	else:
		if c_key != float('-inf'):
			if c_sum > c_max:
				c_res = c_key
				c_max = c_sum
		c_key = key
		c_sum = 1
if c_sum > c_max:
	c_res = c_key
	c_max = c_sum 
print '%s\t%d' %(c_res,c_max)