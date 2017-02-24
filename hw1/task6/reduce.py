#!/usr/bin/python
import sys
import heapq

c_key = float('-inf')
c_sum = 0
top20heap = []
for line in sys.stdin:
	key, value = line.strip().split('\t',1)
	if key == c_key:
		c_sum += 1
	else:
		if c_key != float('-inf'):
			if len(top20heap)<20:
				heapq.heappush(top20heap,(c_sum,c_key))
			elif top20heap[0][0]<c_sum:
				heapq.heappushpop(top20heap,(c_sum,c_key))
		c_key = key
		c_sum = 1
if len(top20heap)<20:
	heapq.heappush(top20heap,(c_sum,c_key))
elif top20heap[0][0]<c_sum:
	heapq.heappushpop(top20heap,(c_sum,c_key)) 

for v,k in sorted(top20heap,reverse=True):
	print '%s\t%d' %(k,v)