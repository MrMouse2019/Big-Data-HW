#!/usr/bin/env python
import sys
from csv import reader

for line in reader(sys.stdin):
    print ('{0}\t{1} {2}'.format(line[2], line[12], 1))
