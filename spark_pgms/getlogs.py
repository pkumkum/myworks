import sys
import os
from operator import add
from pyspark import SparkContext
if __name__ == "__main__":
    sc = SparkContext(appName="urlcount")    
    files = ['/data/sparkpgms/templogdata/'+file  for file in os.listdir('/data/sparkpgms/templogdata') if file.endswith(".log")]
    lines = sc.textFile(','.join(files))
    counts = lines.flatMap(lambda x: x.encode('utf-8','ignore').split(' ')) \
                  .filter(lambda x: '/cms' in x)\
                  .map(lambda x: x.split('?')[0])\
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
    output = counts.collect()
    sc.stop()
    f = open('urldata.txt','w')
    output.sort()
    for i,j in output:
        f.write(i+':'+str(j)+'\n')
