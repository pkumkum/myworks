import sys
from operator import add
from pyspark import SparkContext
from datetime import datetime, timedelta
import datetime

if __name__ == "__main__":
    sc = SparkContext(appName="claimCount")
    lines = sc.textFile(sys.argv[1], 1)    
    ##take enrolid with place of svc code =23 and service date within last 180 days
    counts = lines.map(lambda x: x.split('|')[1] \
    if x.split('|')[11] == '23' and datetime.datetime.strptime(x.split('|')[27], '%Y-%m-%d').date() > \
    datetime.datetime.now().date() + timedelta(days=-180)   else None) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add) 
    output = counts.collect()
    print output
    ##filter for members with count >2
    a = filter(lambda x: x if x[0] != None and x[1] > 2 else None,output)
    print a
    for (word, count) in a:
        print "%s: %i" % (word, count)