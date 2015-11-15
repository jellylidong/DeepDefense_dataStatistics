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
normalFilePath = '/home/worker/workspace/DeepDefense_dataStatistics' + '/csv' + '/topXraw.csv'
normalPath = os.path.join(normalFilePath)
sc.addFile(normalPath);

#attack
attackFilePath = '/home/worker/workspace/DeepDefense_dataStatistics' + '/csv' + '/topXraw.csv'
attackPath = os.path.join(attackFilePath)
sc.addFile(attackPath);


from pyspark import SparkFiles
normalRdd = sc.textFile(SparkFiles.get(normalFilePath))
attackRdd = sc.textFile(SparkFiles.get(attackFilePath))

# src, dst, data_length, protocol_name, protocol_number, arrival_time (len = 6)
normalRaw = normalRdd.map(lambda x: x.split(',')).filter(lambda x: len(x) == 6)
attackRaw = attackRdd.map(lambda x: x.split(',')).filter(lambda x: len(x) == 6)

#(ip, count)
normalTopXSrcIP = normalRaw.map(lambda x:(x[0], 1)).groupByKey().map(lambda (k,v):(k, sum(v))).takeOrdered(10, key = lambda (k,v): -v)
attackTopXSrcIP = attackRaw.map(lambda x:(x[0], 1)).groupByKey().map(lambda (k,v):(k, sum(v))).takeOrdered(10, key = lambda (k,v): -v)

#(ip, count)
normalTopXDstIP = normalRaw.map(lambda x:(x[1], 1)).groupByKey().map(lambda (k,v):(k, sum(v))).takeOrdered(10, key = lambda (k,v): -v)
attackTopXDstIP = attackRaw.map(lambda x:(x[1], 1)).groupByKey().map(lambda (k,v):(k, sum(v))).takeOrdered(10, key = lambda (k,v): -v)

#(ip, data_length)
normalTopXSrcData = normalRaw.map(lambda x:(x[0], float(x[2]))).groupByKey().map(lambda (k,v):(k, sum(v))).takeOrdered(10, key = lambda (k,v): -v)
attackTopXSrcData = attackRaw.map(lambda x:(x[0], float(x[2]))).groupByKey().map(lambda (k,v):(k, sum(v))).takeOrdered(10, key = lambda (k,v): -v)

#(ip, data_length)
normalTopXDstData = normalRaw.map(lambda x:(x[1], float(x[2]))).groupByKey().map(lambda (k,v):(k, sum(v))).takeOrdered(10, key = lambda (k,v): -v)
attackTopXDstData = attackRaw.map(lambda x:(x[1], float(x[2]))).groupByKey().map(lambda (k,v):(k, sum(v))).takeOrdered(10, key = lambda (k,v): -v)

#(protocol_name, count)
normalTopXProtocol = normalRaw.map(lambda x:(x[3], 1)).groupByKey().map(lambda (k,v):(k, sum(v))).takeOrdered(10, key = lambda (k,v): -v)
attackTopXProtocol = attackRaw.map(lambda x:(x[3], 1)).groupByKey().map(lambda (k,v):(k, sum(v))).takeOrdered(10, key = lambda (k,v): -v)

#print "*********************************"
#print "***************", normalTopXSrcData
#print "*********************************"

#topX
normalTopSrcIPSchema = {'IP': map(lambda x: {x[0]: x[1]}, normalTopXSrcIP)}
normalTopDstIPSchema = {'IP': map(lambda x: {x[0]: x[1]}, normalTopXDstIP)}
attackTopSrcIPSchema = {'IP': map(lambda x: {x[0]: x[1]}, attackTopXSrcIP)}
attackTopDstIPSchema = {'IP': map(lambda x: {x[0]: x[1]}, attackTopXDstIP)}


#exit()

#transform the unix time to readable time
import datetime
def unix2readable(t): # t is string
	return datetime.datetime.fromtimestamp(int(float(t))).strftime('%m-%d-%Y %H:%M:%S')



normalTotalData = normalRaw.map(lambda x:float(x[2])).sum()

normalBhvOfTop10Src = []
for x in normalTopXSrcIP:
	ip = x[0]

	sendData = normalRaw.filter(lambda x: x[0] == ip).map(lambda x:float(x[2])).sum()
	receiveData = normalRaw.filter(lambda x: x[1] == ip).map(lambda x:float(x[2])).sum()

	sendPackets = normalRaw.filter(lambda x: x[0] == ip).count()
	receivePackets = normalRaw.filter(lambda x: x[1] == ip).count()

	firstCnt = normalRaw.filter(lambda x: x[0] == ip).map(lambda x: unix2readable(x[-1])).collect()[0]
	lastCnt = normalRaw.filter(lambda x: x[0] == ip).map(lambda x: unix2readable(x[-1])).collect()[-1]

	firstCnted = normalRaw.filter(lambda x: x[1] == ip).map(lambda x: unix2readable(x[-1])).collect()[0]
	lastCnted = normalRaw.filter(lambda x: x[1] == ip).map(lambda x: unix2readable(x[-1])).collect()[-1]

	protocolSrc = normalRaw.filter(lambda x: x[0] == ip).map(lambda x: (x[3], 1)).groupByKey().map(lambda (k,v):(k, sum(v))).map(lambda (k,v):{'Name':k, 'rate':v}).collect()
	protocolDst = normalRaw.filter(lambda x: x[1] == ip).map(lambda x: (x[3], 1)).groupByKey().map(lambda (k,v):(k, sum(v))).map(lambda (k,v):{'Name':k, 'rate':v}).collect()
	normalBhvOfTop10Src.append({'IP': ip,

					'Source': {
						'PacketNumber': sendPackets,
                		'FirstConnection': firstCnt,
                		'LastConnection': lastCnt,
                		'Protocols': protocolSrc,
                		'Country': 'NA', #[{Name: String, rate: Number}],
                		'Hours': 'NA' #[{Hour: Number, rate: Number}]
					},
					'Destination': {
						'PacketNumber': receivePackets,
                		'FirstConnection': firstCnted,
                		'LastConnection': lastCnted,
                		'Protocols': protocolDst,
                		'Country': 'NA', #[{Name: String, rate: Number}],
                		'Hours': 'NA' #[{Hour: Number, rate: Number}]
					}
				})


############ test atom
#print bhvOfTop10Src
#print bhvOfTop10Src

normalBhvOfTop10Dst = []
for x in normalTopXDstIP:
	ip = x[0]

	sendData = normalRaw.filter(lambda x: x[0] == ip).map(lambda x:float(x[2])).sum()
	receiveData = normalRaw.filter(lambda x: x[1] == ip).map(lambda x:float(x[2])).sum()

	sendPackets = normalRaw.filter(lambda x: x[0] == ip).count()
	receivePackets = normalRaw.filter(lambda x: x[1] == ip).count()

	firstCnt = normalRaw.filter(lambda x: x[0] == ip).map(lambda x: unix2readable(x[-1])).collect()[0]
	lastCnt = normalRaw.filter(lambda x: x[0] == ip).map(lambda x: unix2readable(x[-1])).collect()[-1]

	firstCnted = normalRaw.filter(lambda x: x[1] == ip).map(lambda x: unix2readable(x[-1])).collect()[0]
	lastCnted = normalRaw.filter(lambda x: x[1] == ip).map(lambda x: unix2readable(x[-1])).collect()[-1]

	protocolSrc = normalRaw.filter(lambda x: x[0] == ip).map(lambda x: (x[3], 1)).groupByKey().map(lambda (k,v):(k, sum(v))).map(lambda (k,v):{'Name':k, 'rate':v}).collect()
	protocolDst = normalRaw.filter(lambda x: x[1] == ip).map(lambda x: (x[3], 1)).groupByKey().map(lambda (k,v):(k, sum(v))).map(lambda (k,v):{'Name':k, 'rate':v}).collect()
	normalBhvOfTop10Dst.append({'IP': ip,

					'Source': {
						'PacketNumber': sendPackets,
                		'FirstConnection': firstCnt,
                		'LastConnection': lastCnt,
                		'Protocols': protocolSrc,
                		'Country': 'NA', #[{Name: String, rate: Number}],
                		'Hours': 'NA' #[{Hour: Number, rate: Number}]
					},
					'Destination': {
						'PacketNumber': receivePackets,
                		'FirstConnection': firstCnted,
                		'LastConnection': lastCnted,
                		'Protocols': protocolDst,
                		'Country': 'NA', #[{Name: String, rate: Number}],
                		'Hours': 'NA' #[{Hour: Number, rate: Number}]
					}
				})


attackBhvOfTop10Src= []
for x in attackTopXSrcIP:
	ip = x[0]

	sendData = normalRaw.filter(lambda x: x[0] == ip).map(lambda x:float(x[2])).sum()
	receiveData = normalRaw.filter(lambda x: x[1] == ip).map(lambda x:float(x[2])).sum()

	sendPackets = normalRaw.filter(lambda x: x[0] == ip).count()
	receivePackets = normalRaw.filter(lambda x: x[1] == ip).count()

	firstCnt = normalRaw.filter(lambda x: x[0] == ip).map(lambda x: unix2readable(x[-1])).collect()[0]
	lastCnt = normalRaw.filter(lambda x: x[0] == ip).map(lambda x: unix2readable(x[-1])).collect()[-1]

	firstCnted = normalRaw.filter(lambda x: x[1] == ip).map(lambda x: unix2readable(x[-1])).collect()[0]
	lastCnted = normalRaw.filter(lambda x: x[1] == ip).map(lambda x: unix2readable(x[-1])).collect()[-1]

	protocolSrc = normalRaw.filter(lambda x: x[0] == ip).map(lambda x: (x[3], 1)).groupByKey().map(lambda (k,v):(k, sum(v))).map(lambda (k,v):{'Name':k, 'rate':v}).collect()
	protocolDst = normalRaw.filter(lambda x: x[1] == ip).map(lambda x: (x[3], 1)).groupByKey().map(lambda (k,v):(k, sum(v))).map(lambda (k,v):{'Name':k, 'rate':v}).collect()
	attackBhvOfTop10Src.append({'IP': ip,

					'Source': {
						'PacketNumber': sendPackets,
                		'FirstConnection': firstCnt,
                		'LastConnection': lastCnt,
                		'Protocols': protocolSrc,
                		'Country': 'NA', #[{Name: String, rate: Number}],
                		'Hours': 'NA' #[{Hour: Number, rate: Number}]
					},
					'Destination': {
						'PacketNumber': receivePackets,
                		'FirstConnection': firstCnted,
                		'LastConnection': lastCnted,
                		'Protocols': protocolDst,
                		'Country': 'NA', #[{Name: String, rate: Number}],
                		'Hours': 'NA' #[{Hour: Number, rate: Number}]
					}
				})

attackBhvOfTop10Dst= []
for x in attackTopXDstIP:
	ip = x[0]

	sendData = normalRaw.filter(lambda x: x[0] == ip).map(lambda x:float(x[2])).sum()
	receiveData = normalRaw.filter(lambda x: x[1] == ip).map(lambda x:float(x[2])).sum()

	sendPackets = normalRaw.filter(lambda x: x[0] == ip).count()
	receivePackets = normalRaw.filter(lambda x: x[1] == ip).count()

	firstCnt = normalRaw.filter(lambda x: x[0] == ip).map(lambda x: unix2readable(x[-1])).collect()[0]
	lastCnt = normalRaw.filter(lambda x: x[0] == ip).map(lambda x: unix2readable(x[-1])).collect()[-1]

	firstCnted = normalRaw.filter(lambda x: x[1] == ip).map(lambda x: unix2readable(x[-1])).collect()[0]
	lastCnted = normalRaw.filter(lambda x: x[1] == ip).map(lambda x: unix2readable(x[-1])).collect()[-1]

	protocolSrc = normalRaw.filter(lambda x: x[0] == ip).map(lambda x: (x[3], 1)).groupByKey().map(lambda (k,v):(k, sum(v))).map(lambda (k,v):{'Name':k, 'rate':v}).collect()
	protocolDst = normalRaw.filter(lambda x: x[1] == ip).map(lambda x: (x[3], 1)).groupByKey().map(lambda (k,v):(k, sum(v))).map(lambda (k,v):{'Name':k, 'rate':v}).collect()
	attackBhvOfTop10Dst.append({'IP': ip,

					'Source': {
						'PacketNumber': sendPackets,
                		'FirstConnection': firstCnt,
                		'LastConnection': lastCnt,
                		'Protocols': protocolSrc,
                		'Country': 'NA', #[{Name: String, rate: Number}],
                		'Hours': 'NA' #[{Hour: Number, rate: Number}]
					},
					'Destination': {
						'PacketNumber': receivePackets,
                		'FirstConnection': firstCnted,
                		'LastConnection': lastCnted,
                		'Protocols': protocolDst,
                		'Country': 'NA', #[{Name: String, rate: Number}],
                		'Hours': 'NA' #[{Hour: Number, rate: Number}]
					}
				})
