from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader
from operator import add

if __name__ == "__main__":
    sc = SparkContext()
    lines1 = sc.textFile(sys.argv[1], 1)
    lines1 = lines1.mapPartitions(lambda x: reader(x))
    reg_state = lines1.map(lambda x: (("NY" if x[16] == "NY" else "Other"), 1)).reduceByKey(add)
    reg_state = reg_state.map(lambda x: "{0:s}\t{1:d}".format(x[0], x[1]))
    reg_state.saveAsTextFile("task4.out")
    sc.stop()