from sendToMongo import sendToMongo
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

c = {'aaa':ip2CountryLocal('128.101.101.101')}
sendToMongo(c)
