import sys
from operator import add
from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext(appName="claims")
sqlContext = SQLContext(sc)

lines = sc.textFile("E:\claim_data\header1.txt")
parts = lines.map(lambda l: l.split("|"))
claims_header = parts.map(lambda p: {"CLAIM_ID": p[0], "ENROLL_ID":p[1]})

# Infer the schema, and register the SchemaRDD as a table.
# In future versions of PySpark we would like to add support for registering RDDs with other
# datatypes as tables
schemaPeople = sqlContext.inferSchema(claims_header)
schemaPeople.registerAsTable("claims_header")

# SQL can be run over SchemaRDDs that have been registered as a table.
claim_ids = sqlContext.sql("SELECT count(CLAIM_ID) FROM claims_header WHERE ENROLL_ID = HPF71917800 ")

# The results of SQL queries are RDDs and support all the normal RDD operations.
# = teenagers.map(lambda p: "Name: " + p.name)
#for claim_id in claim_ids.collect():
#  print teenName
print claim_ids