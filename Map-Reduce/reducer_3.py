#!/usr/bin/env python
import sys

current_license = None
current_count = 0
current_total = 0
license = None

for line in sys.stdin:
    line = line.strip()
    license, values = line.split('\t', 1)
    amount_due, count = values.split()

    try:
        count = float(count)
        amount_due = float(amount_due)
    except ValueError:
        continue

    if current_license == license:
        current_count += count
        current_total += amount_due
    else:
        if current_license:
            average_due = current_total / current_count
            print ('{0}\t{1:.2f}, {2:.2f}'.format(current_license, current_total, average_due))
        current_license = license
        current_count = count
        current_total = amount_due

if current_license == license:
    average_due = current_total / current_count
    print('{0}\t{1:.2f}, {2:.2f}'.format(current_license, current_total, average_due))



