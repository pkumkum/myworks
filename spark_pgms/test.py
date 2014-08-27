import sys
from operator import add
from pyspark import SparkContext

def parsedat(line):    
   return line.split('|')

	
if __name__ == "__main__":
    sc = SparkContext(appName="claimCount")
    lines = sc.textFile(sys.argv[1], 1)
    counts = parsedat(lines).flatMap(lambda x: x ) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
    output = counts.collect()
    for (word, count) in output:
        print "%s: %i" % (word, count)
