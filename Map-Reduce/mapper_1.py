#!/usr/bin/env python
import sys
import os
from csv import reader

fileName = os.environ.get('mapreduce_map_input_file')

for line in reader(sys.stdin):
    # write the results to stdout
    # the output will be the input for reduce step
    if 'open' in fileName:
        print ('{0:s}\t{1:s}'.format(line[0], 'open'))
    elif 'parking' in fileName:
        print ('{0:s}\t{1:s}, {2:s}, {3:s}, {4:s}'.format(line[0], line[14], line[6], line[2], line[1]))

