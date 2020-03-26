from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader
from operator import add

if __name__ == "__main__":
    sc = SparkContext()
    lines1 = sc.textFile(sys.argv[1], 1)
    lines1 = lines1.mapPartitions(lambda x: reader(x))
    vehicles = lines1.map(lambda x: ((x[14], x[16]), 1)).reduceByKey(add)
    vehicles = vehicles.sortBy(lambda x: x[0][0]).sortBy(lambda x: x[1], False)
    res = sc.parallelize(vehicles.take(20))
    res = res.map(lambda x: "{0:s}, {1:s}\t{2:d}".format(x[0][0], x[0][1], x[1]))
    res.saveAsTextFile("task6.out")
    sc.stop()