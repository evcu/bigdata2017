#!/usr/bin/python
import sys

c_key = float('-inf')
c_acc = []
for line in sys.stdin:
	key, value = line.strip().split('\t',1)
	vals = value.split()
	if key == c_key:
		c_acc[0] += float(vals[0])
		c_acc[1] += int(vals[1])
	else:
		if c_key != float('-inf'):
			print '%s\t%.2f %.2f' %(c_key,c_acc[0],c_acc[0]/c_acc[1])
		c_key = key
		c_sum = [float(vals[0]),int(vals[1])]
print '%s\t%.2f %.2f' %(c_key,c_acc[0],c_acc[0]/c_acc[1])