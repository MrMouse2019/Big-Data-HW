from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    sc = SparkContext()
    lines1 = sc.textFile(sys.argv[1], 1) # parking violations
    lines1 = lines1.mapPartitions(lambda x: reader(x))
    lines2 = sc.textFile(sys.argv[2], 1) # open violations
    lines2 = lines2.mapPartitions(lambda x: reader(x))
    parking = lines1.map(lambda x: (x[0], (x[14], x[6], x[2], x[1])))
    open = lines2.map(lambda x: (x[0], 1))
    res = parking.subtractByKey(open) # res = parking - open
    res = res.sortByKey()
    res = res.map(lambda x: "{0:s}\t{1:s}, {2:s}, {3:s}, {4:s}".format(x[0], x[1][0], x[1][1], x[1][2], x[1][3]))
    res.saveAsTextFile("task1.out")
    sc.stop()