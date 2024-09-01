#!/usr/bin/env python3
import sys

current_word, current_count = None, 0

for line in sys.stdin:
    word, count = line.strip().split('\t', 1)
    try: count = int(count)
    except ValueError: continue

    if current_word == word:
        current_count += count
    else:
        if current_word: print(f'{current_word}\t{current_count}')
        current_word, current_count = word, count

if current_word == word: print(f'{current_word}\t{current_count}')
