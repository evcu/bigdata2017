#!/usr/bin/python
import sys

flag = False
for line in sys.stdin:
	key, value = line.strip().split('\t',1)
	if value and flag:
		print line.strip()
		flag = False
	elif not value:
		flag = True