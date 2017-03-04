
import sys
import os

valcur = None
sorted = 1
for line in sys.stdin:
	key, val = line.split('\t')
	if (valcur):
		if int(val)> int(valcur):
			sys.exit(0)
	else:
		valcur = int(val)
sys.exit(1)
		
