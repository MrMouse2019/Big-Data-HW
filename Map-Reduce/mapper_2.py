#!/usr/bin/env python
import sys
from csv import reader

for line in reader(sys.stdin):
    #print ("%s\t%s" % (line[2], 1))
    print ('{0}\t{1}'.format(line[2], 1))
