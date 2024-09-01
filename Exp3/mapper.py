#!/usr/bin/env python
import sys
for line in sys.stdin:
	line = line.strip()
	words = line.split()
	month = line[10:12]
	daily_max = line[38:45]
	daily_max = daily_max.strip()
	for word in words:
		print ('%s\t%s' % (month ,daily_max))
