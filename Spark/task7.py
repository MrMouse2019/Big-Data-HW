from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader
from operator import add

weekend_days = [5, 6, 12, 13, 19, 20, 26, 27]
num_weekends = 8.0
num_weekdays = 23.0

if __name__ == "__main__":
    sc = SparkContext()
    lines1 = sc.textFile(sys.argv[1], 1)
    lines1 = lines1.mapPartitions(lambda x: reader(x))
    # date_format: YYYY-MM-DD
    num_weekend_violation = lines1.map(lambda x: (x[2], 1 if int(x[1][-2:]) in weekend_days else 0))
    num_weekend_violation = num_weekend_violation.reduceByKey(add)
    avg_num_weekend_violation = num_weekend_violation.map(lambda x: (x[0], float(x[1] / num_weekends)))
    num_weekdays_violation = lines1.map(lambda x: (x[2], 1 if int(x[1][-2:]) not in weekend_days else 0))
    num_weekdays_violation = num_weekdays_violation.reduceByKey(add)
    avg_num_weekdays_violation = num_weekdays_violation.map(lambda x: (x[0], float(x[1] / num_weekdays)))
    res = avg_num_weekend_violation.join(avg_num_weekdays_violation)
    res = res.sortByKey()
    res = res.map(lambda x: "{0}\t{1:.2f}, {2:.2f}".format(x[0], x[1][0], x[1][1]))
    res.saveAsTextFile("task7.out")
    sc.stop()