#!/usr/bin/env python
import sys

current_month = None
current_max = 0

for line in sys.stdin:
    line = line.strip()
    month, daily_max = line.split('\t', 1)
    
    try:
        daily_max = float(daily_max)
    except ValueError:
        continue

    if current_month == month:
        current_max = max(current_max, daily_max)
    else:
        if current_month:
            print(f'{current_month}\t{current_max}')
        current_month, current_max = month, daily_max

if current_month == month:
    print(f'{current_month}\t{current_max}')
	

