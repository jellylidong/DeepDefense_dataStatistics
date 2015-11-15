import os
import sys

#change them when spark and pyspark path changes
SPARK_HOME = "/home/worker/software/spark"
SPARK_HOME_PYTHON = SPARK_HOME + "/python"

os.environ['SPARK_HOME'] = SPARK_HOME
sys.path.append(SPARK_HOME_PYTHON)

from pyspark import SparkContext
from pyspark import SparkConf

sc = SparkContext(appName = 'topXIp')

#X = sys.argv[1]

#normal
normalFilePath = '/home/worker/workspace/DeepDefense' + '/csv' + '/topXraw.csv'
normalPath = os.path.join(normalFilePath)
sc.addFile(normalPath);

#attack
attackFilePath = '/home/worker/workspace/DeepDefense' + '/csv' + '/topXraw.csv'
attackPath = os.path.join(attackFilePath)
sc.addFile(attackPath);


from pyspark import SparkFiles
normalRdd = sc.textFile(SparkFiles.get(normalFilePath))
attackRdd = sc.textFile(SparkFiles.get(attackFilePath))

# src, dst, data_length, protocol_name, protocol_number, arrival_time (len = 6)
normalRaw = normalRdd.map(lambda x: x.split(',')).filter(lambda x: len(x) == 6)
attackRaw = attackRdd.map(lambda x: x.split(',')).filter(lambda x: len(x) == 6)

normalTopXSrcIP = normalRaw.map(lambda x:(x[0], 1)).groupByKey().map(lambda (k,v):(k, sum(v))).takeOrdered(10, key = lambda (k,v): -v)
attackTopXSrcIP = attackRaw.map(lambda x:(x[0], 1)).groupByKey().map(lambda (k,v):(k, sum(v))).takeOrdered(10, key = lambda (k,v): -v)

normalTopXDstIP = normalRaw.map(lambda x:(x[1], 1)).groupByKey().map(lambda (k,v):(k, sum(v))).takeOrdered(10, key = lambda (k,v): -v)
attackTopXDstIP = attackRaw.map(lambda x:(x[1], 1)).groupByKey().map(lambda (k,v):(k, sum(v))).takeOrdered(10, key = lambda (k,v): -v)

normalTopXSrcData = normalRaw.map(lambda x:(x[0], float(x[2]))).groupByKey().map(lambda (k,v):(k, sum(v))).takeOrdered(10, key = lambda (k,v): -v)
attackTopXSrcData = attackRaw.map(lambda x:(x[0], float(x[2]))).groupByKey().map(lambda (k,v):(k, sum(v))).takeOrdered(10, key = lambda (k,v): -v)

normalTopXDstData = normalRaw.map(lambda x:(x[1], float(x[2]))).groupByKey().map(lambda (k,v):(k, sum(v))).takeOrdered(10, key = lambda (k,v): -v)
attackTopXDstData = attackRaw.map(lambda x:(x[1], float(x[2]))).groupByKey().map(lambda (k,v):(k, sum(v))).takeOrdered(10, key = lambda (k,v): -v)

normalTopXProtocol = normalRaw.map(lambda x:(x[3], 1)).groupByKey().map(lambda (k,v):(k, sum(v))).takeOrdered(10, key = lambda (k,v): -v)
attackTopXProtocol = attackRaw.map(lambda x:(x[3], 1)).groupByKey().map(lambda (k,v):(k, sum(v))).takeOrdered(10, key = lambda (k,v): -v)

#print "*********************************"
#print "***************", normalTopXSrcData
#print "*********************************"

#transform the unix time to readable time
import datetime
def unix2readable(t): # t is string
	return datetime.datetime.fromtimestamp(int(t)).strftime('%m-%d-%Y %H:%M:%S')



normalTotalData = normalRaw.map(lambda x:float(x[2])).sum()

bhvOfTop10Src = []
for x in normalTopXSrcIP:
	ip = x[0]

	sendData = normalRaw.filter(lambda x: x[0] == ip).map(lambda x:float(x[2])).sum()
	receiveData = normalRaw.filter(lambda x: x[1] == ip).map(lambda x:float(x[2])).sum()

	sendPackets = normalRaw.filter(lambda x: x[0] == ip).count()
	receivePackets = normalRaw.filter(lambda x: x[1] == ip).count()

	firstCnt = normalRaw.filter(lambda x: x[0] == ip).map(lambda x: unix2readable(x[-1])).collect()[0]
	lastCnt = normalRaw.filter(lambda x: x[0] == ip).map(lambda x: unix2readable(x[-1])).collect()[-1]

	firstCnted = normalRaw.filter(lambda x: x[1] == ip).map(lambda x: unix2readable(x[-1])).collect()[0]
	lasrCnted = normalRaw.filter(lambda x: x[1] == ip).map(lambda x: unix2readable(x[-1])).collect()[-1]

	protocolSrc = normalRaw.filter(lambda x: x[0] == ip).map(lambda x: (x[3], 1)).groupByKey().map(lambda (k,v):(k, sum(v))).collect()
	protocolDst = normalRaw.filter(lambda x: x[1] == ip).map(lambda x: (x[3], 1)).groupByKey().map(lambda (k,v):(k, sum(v))).collect()
	bhvOfTop10Src.append({'IP': ip,

			      'firstConnecting': firstCnt,
			      'lastConnecting': lastCnt,

			      'firstConnected': firstCnted,
			      'lastConnected': lastCnted,

			      'sendData': sendData,
			      'receiveData': receiveData,

			      'sendPacket': sendData,
			      'receivePacket': receiveData,

			      'protocolSrc':protocolSrc,
			      'protocolDst':protocolDst
				})


############
#print bhvOfTop10Src
