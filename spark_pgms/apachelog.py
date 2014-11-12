import sys
import os
from operator import add
from pyspark import SparkContext
if __name__ == "__main__":
    sc = SparkContext(appName="urlcount")    
    files = ['/data/sparkpgms/data/'+file  for file in os.listdir('/data/sparkpgms/data') if file.endswith(".log")]
    lines = sc.textFile(','.join(files))
    line = lines.filter(lambda x : '/cms' in x)
    counts = lines.flatMap(lambda x: x.encode('utf-8','ignore').split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
    output = counts.collect()
    sc.stop()
    for item in output:
        print item[0],item[1] 