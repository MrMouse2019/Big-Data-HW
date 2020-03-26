from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader
from operator import add

if __name__ == "__main__":
    sc = SparkContext()
    lines1 = sc.textFile(sys.argv[1], 1)
    lines1 = lines1.mapPartitions(lambda x: reader(x))
    total = lines1.map(lambda x: (x[2], float(x[12]))).reduceByKey(add)
    license_count = lines1.map(lambda x: (x[2], 1)).reduceByKey(add)
    temp = total.join(license_count)
    # x[0]: license_type    x[1][0]: total  x[1][1]: average
    temp = temp.map(lambda x: (x[0], x[1][0], (x[1][0] / x[1][1])))
    temp = temp.sortByKey()
    temp = temp.map(lambda x: "{0:s}\t{1:.2f}, {2:.2f}".format(x[0], x[1], x[2]))
    temp.saveAsTextFile("task3.out")
    sc.stop()