#!/usr/bin/python
import sys

c_key = float('-inf')
c_acc = 0
for line in sys.stdin:
	key, value = line.strip().split('\t',1)
	if key == c_key:
		c_acc += int(value)
	else:
		if c_key != float('-inf'):
			print '%s\t%d' % (c_key,c_acc)
		c_key = key
		c_acc = int(value)
print '%s\t%d' %(c_key,c_acc)