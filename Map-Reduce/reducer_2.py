#!/usr/bin/env python
import sys

current_vcode = None
current_count = 0
vcode = None # violation code

for line in sys.stdin:
    line = line.strip()
    vcode, count = line.split('\t', 1)

    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently ignore/discard this line
        continue

    if current_vcode == vcode:
        current_count += count
    else:
        if current_vcode:
            #print ('%s\t%s' % (current_vcode, current_count))
            print ('{0}\t{1}'.format(current_vcode, current_count))
        current_vcode = vcode
        current_count = count

if current_vcode == vcode:
    #print('%s\t%s' % (current_vcode, current_count))
    print('{0}\t{1}'.format(current_vcode, current_count))



