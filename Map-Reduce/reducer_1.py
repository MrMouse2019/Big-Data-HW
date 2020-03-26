#!/usr/bin/env python
import sys

current_key = None
current_values = ""
key = None

# input comes from stdin
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    key, values = line.split('\t', 1)

    # the following works because Hadoop sorts map output by key(summons_number) before it is passed to the reducer
    if current_key == key:
        current_values += values
    else:
        if current_key and 'open' not in current_values:
            print ('{0:s}\t{1:s}'.format(current_key, current_values))
        current_key = key
        current_values = values

# do not forget to output the last record if needed!
if current_key == key and 'open' not in current_values:
    print('{0:s}\t{1:s}'.format(current_key, current_values))
