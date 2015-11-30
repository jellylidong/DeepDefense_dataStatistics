import geoip2.database
geoDBpath = '/home/worker/workspace/DeepDefense_dataStatistics/geoDB/GeoLite2-City.mmdb'
reader = geoip2.database.Reader(geoDBpath)

def ip2city(ip):
    try:
        city = reader.city(ip).city.name
    except:
        city = 'not found'
    return city

def ip2la(ip):
    try:
        la = reader.city(ip).location.latitude
    except:
        la = 0.
    return la

def ip2lo(ip):
    try:
        lo = reader.city(ip).location.longitude
    except:
        lo = 0.
    return lo
