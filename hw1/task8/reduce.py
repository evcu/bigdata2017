#!/usr/bin/python
import sys

c_key = float('-inf')
c_sum = 0
all_sum = 0
for line in sys.stdin:
	key, value = line.strip().split('\t',1)
	value = int(value)
	if key == c_key:
		c_sum += value
		all_sum += value
	else:
		if c_key != float('-inf'):
			print '%s\t%d' %(c_key,c_sum)
		c_key = key
		c_sum = value
		all_sum += value
print '%s\t%d' %(c_key,c_sum)
print 'ALLALL\t%d' % all_sum
