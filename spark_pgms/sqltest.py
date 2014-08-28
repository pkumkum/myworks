import sys
from operator import add
from pyspark import SparkContext
from pyspark.sql import SQLContext
#needs Hbase
sc = SparkContext(appName="claims")
sqlContext = SQLContext(sc)

lines = sc.textFile("E:\claim_data\header1.txt")
parts = lines.map(lambda l: l.split("|"))
claims_header = parts.map(lambda p: {"CLAIM_ID": p[0], "ENROLL_ID":p[1]})

schemaPeople = sqlContext.inferSchema(claims_header)
schemaPeople.registerAsTable("claims_header")

claim_ids = sqlContext.sql("SELECT count(CLAIM_ID) FROM claims_header WHERE ENROLL_ID = HPF71917800 ")

print claim_ids