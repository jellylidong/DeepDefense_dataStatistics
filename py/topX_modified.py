import timeit
start = timeit.default_timer()


import os
import sys

#change them when spark and pyspark path changes
SPARK_HOME = "/home/worker/software/spark"
SPARK_HOME_PYTHON = SPARK_HOME + "/python"

os.environ['SPARK_HOME'] = SPARK_HOME
sys.path.append(SPARK_HOME_PYTHON)

from pyspark import SparkContext
from pyspark import SparkConf

#sc = SparkContext(appName = 'topXIp')

#test local speed: only around 75s, much faster
sc = SparkContext('local' , 'topXIp')

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
normalRdd = sc.textFile(SparkFiles.get(normalFilePath)).cache()
attackRdd = sc.textFile(SparkFiles.get(attackFilePath)).cache()

# src, dst, data_length, protocol_name, protocol_number, arrival_time (len = 6)
normalRaw = normalRdd.map(lambda x: x.split(',')).filter(lambda x: len(x) == 6).cache()
attackRaw = attackRdd.map(lambda x: x.split(',')).filter(lambda x: len(x) == 6).cache()

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


protocolCandidates = ['HTTP', 'TCP', 'ICMP', 'UDP']
def protocol2Others(p):
	if not (p in protocolCandidates):
		return 'others'
	return p
def listTuple2Dict(data):
	# list of (k,v)
	res = {}
	res['x'] = len(data)
	for d in data:
		res[d[0]] = d[1]
	return res
#(protocol_name, count) list
normalProtocol = normalRaw.map(lambda x:(protocol2Others(x[3]), 1)).groupByKey().map(lambda (k,v):(k, sum(v))).collect()
attackProtocol = attackRaw.map(lambda x:(protocol2Others(x[3]), 1)).groupByKey().map(lambda (k,v):(k, sum(v))).collect()
ProtocolDistribution = {
	'normal': listTuple2Dict(normalProtocol),
	'attack': listTuple2Dict(attackProtocol),
}


#topXIPs and its number, attack/normal, as src/dst
normalTopSrcIPSchema = {'IP': map(lambda x: {x[0]: x[1]}, normalTopXSrcIP)}
normalTopDstIPSchema = {'IP': map(lambda x: {x[0]: x[1]}, normalTopXDstIP)}
attackTopSrcIPSchema = {'IP': map(lambda x: {x[0]: x[1]}, attackTopXSrcIP)}
attackTopDstIPSchema = {'IP': map(lambda x: {x[0]: x[1]}, attackTopXDstIP)}


#exit()

#transform the unix time to readable time
import datetime
def unix2readable(t): # t is string
	return datetime.datetime.fromtimestamp(int(float(t))).strftime('%m-%d-%Y %H:%M:%S')

# src, dst, data_length, protocol_name, protocol_number, arrival_time (len = 6)
def pktPerSec(rawRdd):
	tmpRdd = rawRdd.map(lambda x: (int(float(x[5])), 1)).groupByKey().map(lambda (k,v): (k,sum(v))).cache()
	rate = tmpRdd.map(lambda (k,v): v).collect()
	#time = tmpRdd.map(lambda (k,v): k-timeStart).collect()
	time = range(tmpRdd.count())
	return (rate, time)

def dataPerSec(rawRdd):
	tmpRdd = rawRdd.map(lambda x: (int(float(x[5])), float(x[2]))).groupByKey().map(lambda (k,v): (k,sum(v))).cache()
	rate = tmpRdd.map(lambda (k,v): v).collect()
#	time = tmpRdd.map(lambda (k,v): k).collect()
	time = range(tmpRdd.count())
	return (rate, time)

def protocolPerSec(rawRdd): #return a dictionary
	res = {}
	# protocolList = rawRdd.map(lambda x: x[3]).distinct().collect()
	protocolList = ['HTTP', 'TCP', 'ICMP', 'UDP', 'others']
	protocolRdd = rawRdd.map(lambda x: (int(float(x[5])), x[3])).cache() #(time, protocol)
	#time = protocolRdd.map(lambda (t, p): t).collect()
	time = range(protocolRdd.groupByKey().count())
	def mapProtocol(p, target):
		if target == 'others':
			if p in protocolList:
				return 0
			else:
				return 1
		else:
			if p == target:
				return 1
			return 0
	for protocol in  protocolList:
		tmpRdd = protocolRdd.map(lambda (t,p): (t, mapProtocol(p, protocol))).groupByKey().map(lambda (k,v): (k, sum(v)))
		rate = tmpRdd.map(lambda (k,v): v).collect()
		#time = tmpRdd.map(lambda (k,v): k).collect()
		res[protocol] = rate
	res['X'] = time
	return res

# normalPR, normalDR, normalX = rateFunc(normalRaw)
# attackPR, attackDR, attackX = rateFunc(attackRaw)
normalPR, normalX = pktPerSec(normalRaw)
normalDR, normalX = dataPerSec(normalRaw)
attackPR, attackX = pktPerSec(attackRaw)
attackDR, attackX = dataPerSec(attackRaw)

# normalProR, normalX = protocolPerSec(normalRaw) #([(pro, rate)], time)
# attackProR, attackX = protocolPerSec(attackRaw)

AveragePacketRate = {
	'normal_X': normalX,
	'attack_X': attackX,
	'normal_Average': float(sum(normalPR))/len(normalPR),
	'attack_Average': float(sum(attackPR))/len(attackPR),
	'normal_min': min(normalPR),
	'attack_min': min(attackPR),
	'normal_max': max(normalPR),
	'attack_max': max(attackPR),
	'normal_Y': normalPR,
	'attack_Y': attackPR
}
#ff = open("AveragePacketRate.txt", "w")
#ff.write(str(AveragePacketRate))
#ff.close()


AveragePacketData = {
	'normal_X': normalX,
	'attack_X': attackX,
	'normal_Average': float(sum(normalDR))/len(normalDR),
	'attack_Average': float(sum(attackDR))/len(attackDR),
	'normal_min': min(normalDR),
	'attack_min': min(attackDR),
	'normal_max': max(normalDR),
	'attack_max': max(attackDR),
	'normal_Y': normalDR,
	'attack_Y': attackDR
}
#ff = open("AveragePacketData.txt", "w")
#ff.write(str(AveragePacketData))
#ff.close()

AverageProtocolRate = {
	'normal': protocolPerSec(normalRaw),
	'attack': protocolPerSec(attackRaw)
}
#ff = open("AverageProtocolRate.txt", "w")
#ff.write(str(AverageProtocolRate))
#ff.close()



normalTotalData = normalRaw.map(lambda x:float(x[2])).sum()

from geoip2 import database
geoDBpath = '/home/worker/workspace/DeepDefense_dataStatistics/geoDB/GeoLite2-City.mmdb'
# geoPath = os.path.join(geoDBpath)
# sc.addFile(geoPath)
readerLocal = database.Reader(geoDBpath)
def ip2CountryLocal(ip):
	try:
		country = readerLocal.city(ip).country.name
	except:
		country = 'not found'
	return country
#
# def ip2CountrySpark(IP):
#     from geoip2 import database
#     def ip2country(ip):
#         try:
# 			country = readerSpark.city(ip).country.name
#         except:
# 			country = 'not found'
#         return city
#
#     readerSpark = database.Reader(SparkFiles.get(geoDBpath))
#     #return [ip2city(ip) for ip in iter]
#     return ip2city(IP)
#
# def TopCountry(normalRawRdd, attackRawRdd):
# 	# src, dst, data_length, protocol_name, protocol_number, arrival_time (len = 6)
# 	normalCountry = normalRawRdd


# normalBhvOfTop10Src = []
# for x in normalTopXSrcIP:
# 	ip = x[0]
#
# 	sendData = normalRaw.filter(lambda x: x[0] == ip).map(lambda x:float(x[2])).sum()
# 	receiveData = normalRaw.filter(lambda x: x[1] == ip).map(lambda x:float(x[2])).sum()
#
# 	sendPackets = normalRaw.filter(lambda x: x[0] == ip).count()
# 	receivePackets = normalRaw.filter(lambda x: x[1] == ip).count()
#
# 	firstCnt = normalRaw.filter(lambda x: x[0] == ip).map(lambda x: unix2readable(x[-1])).collect()[0]
# 	lastCnt = normalRaw.filter(lambda x: x[0] == ip).map(lambda x: unix2readable(x[-1])).collect()[-1]
#
# 	firstCnted = normalRaw.filter(lambda x: x[1] == ip).map(lambda x: unix2readable(x[-1])).collect()[0]
# 	lastCnted = normalRaw.filter(lambda x: x[1] == ip).map(lambda x: unix2readable(x[-1])).collect()[-1]
#
# 	protocolSrc = normalRaw.filter(lambda x: x[0] == ip).map(lambda x: (x[3], 1)).groupByKey().map(lambda (k,v):(k, sum(v))).map(lambda (k,v):{'Name':k, 'rate':v}).collect()
# 	protocolDst = normalRaw.filter(lambda x: x[1] == ip).map(lambda x: (x[3], 1)).groupByKey().map(lambda (k,v):(k, sum(v))).map(lambda (k,v):{'Name':k, 'rate':v}).collect()
# 	normalBhvOfTop10Src.append({'IP': ip,
#
# 					'Source': {
# 						'PacketNumber': sendPackets,
#                 		'FirstConnection': firstCnt,
#                 		'LastConnection': lastCnt,
#                 		'Protocols': protocolSrc,
#                 		'Country': 'NA', #[{Name: String, rate: Number}],
#                 		'Hours': 'NA' #[{Hour: Number, rate: Number}]
# 					},
# 					'Destination': {
# 						'PacketNumber': receivePackets,
#                 		'FirstConnection': firstCnted,
#                 		'LastConnection': lastCnted,
#                 		'Protocols': protocolDst,
#                 		'Country': 'NA', #[{Name: String, rate: Number}],
#                 		'Hours': 'NA' #[{Hour: Number, rate: Number}]
# 					}
# 				})
#
#
# ############ test atom
# #print bhvOfTop10Src
# #print bhvOfTop10Src
#
# normalBhvOfTop10Dst = []
# for x in normalTopXDstIP:
# 	ip = x[0]
#
# 	sendData = normalRaw.filter(lambda x: x[0] == ip).map(lambda x:float(x[2])).sum()
# 	receiveData = normalRaw.filter(lambda x: x[1] == ip).map(lambda x:float(x[2])).sum()
#
# 	sendPackets = normalRaw.filter(lambda x: x[0] == ip).count()
# 	receivePackets = normalRaw.filter(lambda x: x[1] == ip).count()
#
# 	firstCnt = normalRaw.filter(lambda x: x[0] == ip).map(lambda x: unix2readable(x[-1])).collect()[0]
# 	lastCnt = normalRaw.filter(lambda x: x[0] == ip).map(lambda x: unix2readable(x[-1])).collect()[-1]
#
# 	firstCnted = normalRaw.filter(lambda x: x[1] == ip).map(lambda x: unix2readable(x[-1])).collect()[0]
# 	lastCnted = normalRaw.filter(lambda x: x[1] == ip).map(lambda x: unix2readable(x[-1])).collect()[-1]
#
# 	protocolSrc = normalRaw.filter(lambda x: x[0] == ip).map(lambda x: (x[3], 1)).groupByKey().map(lambda (k,v):(k, sum(v))).map(lambda (k,v):{'Name':k, 'rate':v}).collect()
# 	protocolDst = normalRaw.filter(lambda x: x[1] == ip).map(lambda x: (x[3], 1)).groupByKey().map(lambda (k,v):(k, sum(v))).map(lambda (k,v):{'Name':k, 'rate':v}).collect()
# 	normalBhvOfTop10Dst.append({'IP': ip,
#
# 					'Source': {
# 						'PacketNumber': sendPackets,
#                 		'FirstConnection': firstCnt,
#                 		'LastConnection': lastCnt,
#                 		'Protocols': protocolSrc,
#                 		'Country': 'NA', #[{Name: String, rate: Number}],
#                 		'Hours': 'NA' #[{Hour: Number, rate: Number}]
# 					},
# 					'Destination': {
# 						'PacketNumber': receivePackets,
#                 		'FirstConnection': firstCnted,
#                 		'LastConnection': lastCnted,
#                 		'Protocols': protocolDst,
#                 		'Country': 'NA', #[{Name: String, rate: Number}],
#                 		'Hours': 'NA' #[{Hour: Number, rate: Number}]
# 					}
# 				})
#
#
# attackBhvOfTop10Src= []
# for x in attackTopXSrcIP:
# 	ip = x[0]
#
# 	sendData = normalRaw.filter(lambda x: x[0] == ip).map(lambda x:float(x[2])).sum()
# 	receiveData = normalRaw.filter(lambda x: x[1] == ip).map(lambda x:float(x[2])).sum()
#
# 	sendPackets = normalRaw.filter(lambda x: x[0] == ip).count()
# 	receivePackets = normalRaw.filter(lambda x: x[1] == ip).count()
#
# 	firstCnt = normalRaw.filter(lambda x: x[0] == ip).map(lambda x: unix2readable(x[-1])).collect()[0]
# 	lastCnt = normalRaw.filter(lambda x: x[0] == ip).map(lambda x: unix2readable(x[-1])).collect()[-1]
#
# 	firstCnted = normalRaw.filter(lambda x: x[1] == ip).map(lambda x: unix2readable(x[-1])).collect()[0]
# 	lastCnted = normalRaw.filter(lambda x: x[1] == ip).map(lambda x: unix2readable(x[-1])).collect()[-1]
#
# 	protocolSrc = normalRaw.filter(lambda x: x[0] == ip).map(lambda x: (x[3], 1)).groupByKey().map(lambda (k,v):(k, sum(v))).map(lambda (k,v):{'Name':k, 'rate':v}).collect()
# 	protocolDst = normalRaw.filter(lambda x: x[1] == ip).map(lambda x: (x[3], 1)).groupByKey().map(lambda (k,v):(k, sum(v))).map(lambda (k,v):{'Name':k, 'rate':v}).collect()
# 	attackBhvOfTop10Src.append({'IP': ip,
#
# 					'Source': {
# 						'PacketNumber': sendPackets,
#                 		'FirstConnection': firstCnt,
#                 		'LastConnection': lastCnt,
#                 		'Protocols': protocolSrc,
#                 		'Country': 'NA', #[{Name: String, rate: Number}],
#                 		'Hours': 'NA' #[{Hour: Number, rate: Number}]
# 					},
# 					'Destination': {
# 						'PacketNumber': receivePackets,
#                 		'FirstConnection': firstCnted,
#                 		'LastConnection': lastCnted,
#                 		'Protocols': protocolDst,
#                 		'Country': 'NA', #[{Name: String, rate: Number}],
#                 		'Hours': 'NA' #[{Hour: Number, rate: Number}]
# 					}
# 				})
#
# attackBhvOfTop10Dst= []
# for x in attackTopXDstIP:
# 	ip = x[0]
#
# 	sendData = normalRaw.filter(lambda x: x[0] == ip).map(lambda x:float(x[2])).sum()
# 	receiveData = normalRaw.filter(lambda x: x[1] == ip).map(lambda x:float(x[2])).sum()
#
# 	sendPackets = normalRaw.filter(lambda x: x[0] == ip).count()
# 	receivePackets = normalRaw.filter(lambda x: x[1] == ip).count()
#
# 	firstCnt = normalRaw.filter(lambda x: x[0] == ip).map(lambda x: unix2readable(x[-1])).collect()[0]
# 	lastCnt = normalRaw.filter(lambda x: x[0] == ip).map(lambda x: unix2readable(x[-1])).collect()[-1]
#
# 	firstCnted = normalRaw.filter(lambda x: x[1] == ip).map(lambda x: unix2readable(x[-1])).collect()[0]
# 	lastCnted = normalRaw.filter(lambda x: x[1] == ip).map(lambda x: unix2readable(x[-1])).collect()[-1]
#
# 	protocolSrc = normalRaw.filter(lambda x: x[0] == ip).map(lambda x: (x[3], 1)).groupByKey().map(lambda (k,v):(k, sum(v))).map(lambda (k,v):{'Name':k, 'rate':v}).collect()
# 	protocolDst = normalRaw.filter(lambda x: x[1] == ip).map(lambda x: (x[3], 1)).groupByKey().map(lambda (k,v):(k, sum(v))).map(lambda (k,v):{'Name':k, 'rate':v}).collect()
# 	attackBhvOfTop10Dst.append({'IP': ip,
#
# 					'Source': {
# 						'PacketNumber': sendPackets,
#                 		'FirstConnection': firstCnt,
#                 		'LastConnection': lastCnt,
#                 		'Protocols': protocolSrc,
#                 		'Country'128.101.101.101'': 'NA', #[{Name: String, rate: Number}],
#                 		'Hours': 'NA' #[{Hour: Number, rate: Number}]
# 					},
# 					'Destination': {
# 						'PacketNumber': receivePackets,
#                 		'FirstConnection': firstCnted,
#                 		'LastConnection': lastCnted,
#                 		'Protocols': protocolDst,
#                 		'Country': 'NA', #[{Name: String, rate: Number}],
#                 		'Hours': 'NA' #[{Hour: Number, rate: Number}]
# 					}
# 				})

def bhvOfTop10IP(rawRdd, IPs):
	#rawRdd = normalRaw or attackRaw
	#IPs = attackTopXDstIP, (attack/normal, Src/Dst)
	result= []
	for x in IPs:
		ip = x[0]
		srcRdd = rawRdd.filter(lambda x: x[0] == ip).cache()
		dstRdd = rawRdd.filter(lambda x: x[1] == ip).cache()
		sendData = srcRdd.map(lambda x:float(x[2])).sum()
		receiveData = dstRdd.map(lambda x:float(x[2])).sum()

		sendPackets = srcRdd.count()
		receivePackets = dstRdd.count()

		firstCnt = srcRdd.map(lambda x: unix2readable(x[-1])).collect()[0]
		lastCnt = dstRdd.map(lambda x: unix2readable(x[-1])).collect()[-1]

		firstCnted = srcRdd.map(lambda x: unix2readable(x[-1])).collect()[0]
		lastCnted = dstRdd.map(lambda x: unix2readable(x[-1])).collect()[-1]

		protocolSrc = srcRdd.map(lambda x: (x[3], 1)).groupByKey().map(lambda (k,v):(k, sum(v))).map(lambda (k,v):{'Name':k, 'rate':v}).collect()
		protocolDst = dstRdd.map(lambda x: (x[3], 1)).groupByKey().map(lambda (k,v):(k, sum(v))).map(lambda (k,v):{'Name':k, 'rate':v}).collect()
		# country = []
		# country.append(ip2CountryLocal(ip))
		result.append({'IP': ip,

						'Source': {
							'PacketNumber': sendPackets,
							'sendData': sendData,
	                		'FirstConnection': firstCnt,
	                		'LastConnection': lastCnt,
	                		'Protocols': protocolSrc,
	                		'Country': {'name':ip2CountryLocal(ip)}, #[{Name: String, rate: Number}],
	                		'Hours': 'NA' #[{Hour: Number, rate: Number}]
						},
						'Destination': {
							'PacketNumber': receivePackets,
							'receiveData': receiveData,
	                		'FirstConnection': firstCnted,
	                		'LastConnection': lastCnted,
	                		'Protocols': protocolDst,
	                		'Country': {'name':ip2CountryLocal(ip)}, #[{Name: String, rate: Number}],
	                		'Hours': 'NA' #[{Hour: Number, rate: Number}]
						}
					})
	return result

attackBehaviorOfSource = bhvOfTop10IP(attackRaw, attackTopXSrcIP)
attackBehaviorOfDestination = bhvOfTop10IP(attackRaw, attackTopXDstIP)
normalBehaviorOfSource = bhvOfTop10IP(normalRaw, normalTopXSrcIP)
normalBehaviorOfDestination = bhvOfTop10IP(normalRaw, normalTopXDstIP)
ff = open("attackBehaviorOfSource.txt", "w")
ff.write(str(attackBehaviorOfSource))
ff.close()
BehaviorOfTopIP = {
	'attackBehaviorOfSource': attackBehaviorOfSource,
	'attackBehaviorOfDestination': attackBehaviorOfDestination,
	'normalBehaviorOfSource': normalBehaviorOfSource,
	'normalBehaviorOfDestination': normalBehaviorOfDestination,
}

from ip2x import ip2city, ip2la, ip2lo
# src, dst, data_length, protocol_name, protocol_number, arrival_time (len = 6)
#normalRaw = normalRdd.map(lambda x: x.split(',')).filter(lambda x: len(x) == 6).cache()

normalSrc = normalRaw.map(lambda x: x[0])
normalDst = normalRaw.map(lambda x: x[1])
attackSrc = attackRaw.map(lambda x: x[0])
attackDst = attackRaw.map(lambda x: x[1])
ff = open('normalDst.txt', 'w')
ff.write(str(normalDst.collect()))
ff.close()

allIPs = normalSrc.union(normalDst).union(attackSrc).union(attackDst).distinct().collect()
geoData = []
for ip in allIPs:
    lo = ip2lo(ip)
    la = ip2la(ip)
    geoData.append({'ip' : ip,
                    'longitude' : lo,
                    'lantitude' : la})

attackNum = 2000
normalNum = 2000
victimNum = 2000

ff = open('allIPs.txt', 'w')
ff.write(str(allIPs))
ff.close()

def topCommunication(rawRdd, num, resType):
	ipPairRdd = rawRdd.map(lambda x: x[0] + ' ' + x[1]).cache() #'src dst'
	ipPairRddCount = (ipPairRdd.map(lambda x: (x, 1)).groupByKey().map(lambda (k,v): (k, sum(v))). #([src, dst], v)
					  map(lambda (k,v):(k.split(' '), v)).cache())
	values = ipPairRddCount.map(lambda (k,v):v).collect()
	maxValue = max(values)
	minValue = min(values)
	rangeValue = maxValue - minValue
	if resType == 'DS':
		DS = []
		data = ipPairRddCount.takeOrdered(num, key = lambda x: -x[1])
		for x in data:
			src = x[0][0]
			dst = x[0][1]
			val = x[1]
			DS.append({'source': src,
		                'destination' : dst,
		                'value' : 100*(maxValue-val)/rangeValue})
		return DS
	if resType == 'VIC':
		VIC = []
		data = ipPairRddCount.takeOrdered(num, key = lambda x: -x[1])
		for x in data:
			dst = x[0][1]
			val = x[1]
			VIC.append({'destination' : dst,
                        'value' :100*(maxValue-val)/rangeValue})
		return VIC


attackDS = topCommunication(attackRaw, attackNum, 'DS')
normalDS = topCommunication(normalRaw, normalNum, 'DS')
victimData = topCommunication(attackRaw, victimNum, 'VIC')
ff = open('geoData.txt', 'w')
ff.write(str(geoData))
ff.close()
ipDistribution = {#'recordID': 1,#sys.argv[1], #add arg!!!!!!!!
                  'geoData' : geoData,
                  'attackDS' : attackDS,
                  'normalDS' : normalDS,
                  'victimData' : victimData}

sc.stop()

StatisticsSchema = {
	'user': 'NA',
	'AveragePacketRate': AveragePacketRate,
	'AveragePacketData': AveragePacketData,
	'AverageProtocolRate': AverageProtocolRate,
	'ProtocolDistribution': ProtocolDistribution,
	'BehaviorOfTopIP': BehaviorOfTopIP,
	'Map': ipDistribution
}

from sendToMongo import sendToMongo
sendToMongo(StatisticsSchema)


stop = timeit.default_timer()
print 'total running time: ', stop - start

# print "********************************"
# print "********************************"
# print "********************************"
# print StatisticsSchema
