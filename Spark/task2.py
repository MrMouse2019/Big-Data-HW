from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader
from operator import add

if __name__ == "__main__":
    sc = SparkContext()
    lines1 = sc.textFile(sys.argv[1], 1) # parking violations
    lines1 = lines1.mapPartitions(lambda x: reader(x))
    parking_freq = lines1.map(lambda x: (x[2], 1)).reduceByKey(add)
    parking_freq = parking_freq.sortByKey()
    res = parking_freq.map(lambda x: "{0}\t{1:d}".format(x[0], x[1]))
    res.saveAsTextFile("task2.out")
    sc.stop()