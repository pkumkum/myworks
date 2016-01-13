import sys
from operator import add
from pyspark import SparkContext
from datetime import datetime, timedelta
import datetime
from pyspark.sql import SQLContext, Row

#test comment1
sc = SparkContext(appName="claimCount")
sqlContext = SQLContext(sc)
lines = sc.textFile("/data/claim_data/claimdata.txt")
parts = lines.map(lambda l: l.split("|"))
claims = parts.map(lambda p: Row(enroll_id = p[2],claim_id =p[1], place_of_service_cd=p[12],diag_cd =p[75],diag_position = p[73],procedure_cd=p[47]))
schemaPeople = sqlContext.inferSchema(claimdata)
schemaPeople.registerTempTable("people")
members = sqlContext.sql("SELECT enroll_id FROM claims WHERE place_of_service_cd ='23' AND diag_cd = '4423' and diag_position='1' and procedure_cd= 'A9579'")
enroll_ids = members.map(lambda p: p.enroll_id)
for enroll_id in enroll_ids.collect():
  print enroll_id
