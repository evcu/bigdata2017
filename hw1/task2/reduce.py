#!/usr/bin/python

c_key = float('-inf')
c_sum = 0
for line in sys.stdin:
	key, value = line.strip().split('\t',1)
	if key == c_key:
		c_sum += 1
	else:
		if c_key != float('-inf'):
			print '%s\t%d' %(c_key,c_sum)
		c_key = key
		c_sum = 1