import os
import sys

#change them when spark and pyspark path changes
SPARK_HOME = "/home/worker/software/spark"
SPARK_HOME_PYTHON = SPARK_HOME + "/python"

os.environ['SPARK_HOME'] = SPARK_HOME
sys.path.append(SPARK_HOME_PYTHON)

from pyspark import SparkContext
from pyspark import SparkConf

#sc = SparkContext(appName = 'testGeoSpark')
sc = SparkContext('local', 'testGeoSpark')

#X = sys.argv[1]

#normal
normalFilePath = '/home/worker/workspace/DeepDefense_dataStatistics' + '/csv' + '/topXraw.csv'
normalPath = os.path.join(normalFilePath)
sc.addFile(normalPath);

#attack
attackFilePath = '/home/worker/workspace/DeepDefense_dataStatistics' + '/csv' + '/topXraw.csv'
attackPath = os.path.join(attackFilePath)
sc.addFile(attackPath)

from pyspark import SparkFiles
normalRdd = sc.textFile(SparkFiles.get(normalFilePath))
attackRdd = sc.textFile(SparkFiles.get(attackFilePath))


import geoip2.database
geoDBpath = '/home/worker/workspace/geoDB/GeoLite2-City.mmdb'
geoPath = os.path.join(geoDBpath)
sc.addFile(geoPath)
#reader = geoip2.database.Reader(SparkFiles.get(geoPath))
#reader = geoip2.database.Reader('GeoLite2-City.mmdb')

# def ip2city(ip):
#     try:
#         city = reader.city(ip).city.name
#     except:
#         city = 'not found'
#     return city

def partitionIp2city(iter):
    from geoip2 import database

    def ip2city(ip):
        try:
           city = reader.city(ip).city.name
        except:
            city = 'not found'
        return city

    reader = database.Reader(SparkFiles.get(geoDBpath))
    #return [ip2city(ip) for ip in iter]
    return ip2city(iter)
print "****************************************"
print "*****************", SparkFiles.get(geoPath)
#print reader.city("128.101.101.101").city.name
raw = normalRdd.map(lambda x: x.split(',')).map(lambda x: x[0])
print "****************************************", raw.take(10)
raw2 = raw.map(lambda x: partitionIp2city(x))
print "****************************************", raw2.take(10)
# raw2 = map(lambda x: (x[0], ip2city(x[0])), raw.collect())
# raw3 = sc.parallelize(raw2)
# print raw3.take(10)
# print "******************************"
# #print raw.take(10)

#import geoip2.database
#reader = geoip2.database.Reader('GeoLite2-City.mmdb')
#print reader.city("128.101.101.101").city.name
